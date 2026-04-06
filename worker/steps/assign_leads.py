import io
import unicodedata
from difflib import SequenceMatcher

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.config import get_settings
from shared.logging_config import get_logger
from shared.minio_client import MinioClient

logger = get_logger(__name__)


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
        logger.info("CARTEIRA_MINIO_KEY não configurado, pulando atribuição de leads", extra={"job_id": job_id, "step": "assign_leads", "event": "assign_leads_skip"})
        return

    # Baixa carteira do MinIO
    try:
        minio = MinioClient()
        if not minio.object_exists(carteira_key):
            logger.warning("Carteira não encontrada no MinIO (%s) — pulando atribuição de leads", carteira_key, extra={"job_id": job_id, "step": "assign_leads", "event": "assign_leads_warning"})
            return
        file_bytes = minio.download_file(carteira_key)
    except Exception as exc:
        logger.warning("Erro ao baixar carteira do MinIO (%s): %s — pulando atribuição de leads", carteira_key, exc, extra={"job_id": job_id, "step": "assign_leads", "event": "assign_leads_warning"})
        return

    # Carrega xlsx
    df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    if "CD_CPF_CNPJ_CLIENTE" not in df.columns or "Relacionamento" not in df.columns:
        logger.error("Carteira xlsx não tem colunas esperadas (CD_CPF_CNPJ_CLIENTE, Relacionamento) — pulando", extra={"job_id": job_id, "step": "assign_leads", "event": "assign_leads_warning"})
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
        extra={"job_id": job_id, "step": "assign_leads", "event": "assign_leads_loaded"},
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
            extra={"job_id": job_id, "step": "assign_leads", "event": "assign_unmatched"},
        )
    logger.info("Assignments a processar: %d", len(assignments), extra={"job_id": job_id, "step": "assign_leads", "event": "assign_leads_matched"})

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
        extra={"job_id": job_id, "step": "assign_leads", "event": "assign_leads_lookup"},
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

    # Fallback: CNPJs novos em projetinho_pai sem atribuição e sem carteira → usa NOME_CONSULTOR
    insert_rows = _fallback_by_nome_consultor(session, job_id, insert_rows, usuario_norm_map, lider_map)

    if not insert_rows:
        logger.info("Nenhum lead novo para atribuir", extra={"job_id": job_id, "step": "assign_leads", "event": "assign_leads_done"})
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
        extra={"job_id": job_id, "step": "assign_leads", "event": "assign_leads_done"},
    )


def _fallback_by_nome_consultor(
    session: Session,
    job_id: str,
    existing_rows: list[dict],
    usuario_norm_map: dict[str, str],
    lider_map: dict[str, str | None],
) -> list[dict]:
    """Complementa insert_rows com CNPJs que estão em projetinho_pai mas não têm
    atribuição em lead_atribuicoes nem foram encontrados na carteira.

    Usa NOME_CONSULTOR de projetinho_pai como fonte para fuzzy match de usuario.
    """
    # lead_ids já cobertos pela carteira
    covered_lead_ids = {r["lead_id"] for r in existing_rows}

    # Busca todos os leads sem atribuição
    unassigned = session.execute(
        text("""
            SELECT pp.id, pp."NOME_CONSULTOR"
            FROM public.projetinho_pai pp
            WHERE NOT EXISTS (
                SELECT 1 FROM public.lead_atribuicoes la WHERE la.lead_id = pp.id
            )
            AND pp."NOME_CONSULTOR" IS NOT NULL
            AND pp."NOME_CONSULTOR" <> ''
        """)
    ).fetchall()

    if not unassigned:
        return existing_rows

    logger.info(
        "Fallback NOME_CONSULTOR: %d leads sem atribuição para processar",
        len(unassigned),
        extra={"job_id": job_id, "step": "assign_leads", "event": "assign_fallback_start"},
    )

    fallback_rows = []
    unmatched: set[str] = set()
    for lead_id, nome_consultor in unassigned:
        if lead_id in covered_lead_ids:
            continue
        uid = _match_name(nome_consultor, usuario_norm_map)
        if uid is None:
            unmatched.add(nome_consultor)
            continue
        lid = lider_map.get(uid)
        fallback_rows.append({
            "lead_id": lead_id,
            "consultor_id": uid,
            "lider_id": lid,
            "atribuido_por": lid,
        })

    if unmatched:
        logger.warning(
            "Fallback: %d NOME_CONSULTOR sem match em usuarios (ignorados): %s",
            len(unmatched),
            sorted(unmatched)[:20],
            extra={"job_id": job_id, "step": "assign_leads", "event": "assign_fallback_unmatched"},
        )

    logger.info(
        "Fallback NOME_CONSULTOR: %d atribuições adicionais geradas",
        len(fallback_rows),
        extra={"job_id": job_id, "step": "assign_leads", "event": "assign_fallback_done"},
    )

    return existing_rows + fallback_rows
