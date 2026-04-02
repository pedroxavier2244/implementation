import io
import logging
import unicodedata
from difflib import SequenceMatcher

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.config import get_settings
from shared.minio_client import MinioClient

logger = logging.getLogger(__name__)


def _norm(name: str) -> str:
    """Normaliza nome: remove acentos, lowercase, colapsa espaços."""
    s = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode("ascii")
    return " ".join(s.lower().split())


def _match_name(carteira_name: str, usuario_norm_map: dict[str, str]) -> str | None:
    """Retorna o id do usuario que melhor corresponde ao nome da carteira.

    Tenta, em ordem:
    1. Match exato normalizado
    2. Match por subconjunto de palavras (ex. 'ana maia' ⊂ 'ana caroline maia')
    3. Fuzzy match com SequenceMatcher >= 0.82
    """
    norm = _norm(carteira_name)

    # 1. Exact
    if norm in usuario_norm_map:
        return usuario_norm_map[norm]

    # 2. Subset de palavras
    norm_words = set(norm.split())
    for db_norm, uid in usuario_norm_map.items():
        db_words = set(db_norm.split())
        if norm_words <= db_words or db_words <= norm_words:
            return uid

    # 3. Fuzzy
    best_score = 0.0
    best_uid = None
    for db_norm, uid in usuario_norm_map.items():
        score = SequenceMatcher(None, norm, db_norm).ratio()
        if score > best_score:
            best_score = score
            best_uid = uid

    if best_score >= 0.82:
        return best_uid

    return None


def run_assign_leads(session: Session, job_id: str) -> None:
    """Atribui leads da carteira aos consultores em lead_atribuicoes.

    - Baixa carteira.xlsx do MinIO (key configurável via CARTEIRA_MINIO_KEY)
    - Deduplica por CNPJ (última ocorrência vence)
    - Faz match de nome da carteira → usuarios.id (com fuzzy fallback)
    - Insere em lead_atribuicoes ON CONFLICT (lead_id) DO NOTHING
      (não altera atribuições existentes)
    - Skipa silenciosamente se o arquivo não existe no MinIO
    """
    settings = get_settings()
    carteira_key = settings.CARTEIRA_MINIO_KEY
    if not carteira_key:
        logger.info("CARTEIRA_MINIO_KEY não configurado, pulando atribuição de leads")
        return

    # Baixa carteira do MinIO
    try:
        minio = MinioClient()
        if not minio.object_exists(carteira_key):
            logger.warning("Carteira não encontrada no MinIO (%s) — pulando atribuição de leads", carteira_key)
            return
        file_bytes = minio.download_file(carteira_key)
    except Exception as exc:
        logger.warning("Erro ao baixar carteira do MinIO (%s): %s — pulando atribuição de leads", carteira_key, exc)
        return

    # Carrega xlsx
    df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    if "CD_CPF_CNPJ_CLIENTE" not in df.columns or "Relacionamento" not in df.columns:
        logger.error("Carteira xlsx não tem colunas esperadas (CD_CPF_CNPJ_CLIENTE, Relacionamento) — pulando")
        return

    # Normaliza CNPJ: remove sufixo '.0' de leitura float, mantém só dígitos
    def _norm_cnpj(v: str) -> str | None:
        if pd.isna(v) or not str(v).strip():
            return None
        digits = "".join(c for c in str(v).strip().split(".")[0] if c.isdigit())
        return digits if digits else None

    df["cnpj_norm"] = df["CD_CPF_CNPJ_CLIENTE"].map(_norm_cnpj)
    df = df.dropna(subset=["cnpj_norm", "Relacionamento"])
    df = df[df["Relacionamento"].str.strip() != ""]

    # Deduplica: última ocorrência por CNPJ vence
    df = df.drop_duplicates(subset="cnpj_norm", keep="last")

    cnpj_to_consultor: dict[str, str] = dict(
        zip(df["cnpj_norm"], df["Relacionamento"].str.strip())
    )
    logger.info(
        "Carteira carregada: %d CNPJs únicos, %d consultores únicos",
        len(cnpj_to_consultor),
        len(set(cnpj_to_consultor.values())),
    )

    if not cnpj_to_consultor:
        return

    # Carrega usuarios: id, nome, lider_id
    usuario_rows = session.execute(
        text("SELECT id, nome, lider_id FROM public.usuarios")
    ).fetchall()

    usuario_norm_map: dict[str, str] = {_norm(str(r[1])): str(r[0]) for r in usuario_rows}
    lider_map: dict[str, str | None] = {
        str(r[0]): (str(r[2]) if r[2] else None) for r in usuario_rows
    }

    # Faz match carteira → consultor_id
    assignments: list[tuple[str, str, str | None]] = []
    unmatched: set[str] = set()
    for cnpj, name in cnpj_to_consultor.items():
        uid = _match_name(name, usuario_norm_map)
        if uid is None:
            unmatched.add(name)
        else:
            lid = lider_map.get(uid)
            assignments.append((cnpj, uid, lid))

    if unmatched:
        logger.warning(
            "Consultores da carteira sem match em usuarios (ignorados): %s",
            sorted(unmatched),
        )
    logger.info("Assignments a processar: %d", len(assignments))

    if not assignments:
        return

    # Busca projetinho_pai.id para cada CNPJ
    cnpj_list = [a[0] for a in assignments]
    lead_rows = session.execute(
        text(
            'SELECT "CD_CPF_CNPJ_CLIENTE", id FROM public.projetinho_pai'
            ' WHERE "CD_CPF_CNPJ_CLIENTE" = ANY(:cnpjs)'
        ),
        {"cnpjs": cnpj_list},
    ).fetchall()
    cnpj_to_lead_id: dict[str, int] = {str(r[0]): r[1] for r in lead_rows}

    logger.info(
        "CNPJs encontrados em projetinho_pai: %d / %d",
        len(cnpj_to_lead_id),
        len(cnpj_list),
    )

    # Monta linhas a inserir
    insert_rows = []
    for cnpj, consultor_id, lider_id in assignments:
        lead_id = cnpj_to_lead_id.get(cnpj)
        if lead_id is None:
            continue
        insert_rows.append(
            {
                "lead_id": lead_id,
                "consultor_id": consultor_id,
                "lider_id": lider_id,
                "atribuido_por": lider_id,
            }
        )

    if not insert_rows:
        logger.info("Nenhum lead novo para atribuir")
        return

    # Insere em batches de 500: ON CONFLICT (lead_id) DO NOTHING preserva atribuições existentes
    inserted_total = 0
    chunk_size = 500
    for i in range(0, len(insert_rows), chunk_size):
        chunk = insert_rows[i : i + chunk_size]
        result = session.execute(
            text("""
                INSERT INTO public.lead_atribuicoes
                    (id, lead_id, consultor_id, lider_id, atribuido_por, data_atribuicao, liberado)
                VALUES
                    (gen_random_uuid(), :lead_id, :consultor_id, :lider_id, :atribuido_por, NOW(), false)
                ON CONFLICT (lead_id) DO NOTHING
            """),
            chunk,
        )
        inserted_total += result.rowcount

    session.commit()
    skipped = len(insert_rows) - inserted_total
    logger.info(
        "Lead assignments: %d inseridos, %d já existiam (preservados)",
        inserted_total,
        skipped,
    )
