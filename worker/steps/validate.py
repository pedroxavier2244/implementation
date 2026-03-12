import uuid
import re
import unicodedata

from sqlalchemy.orm import Session

from shared.config import get_settings
from shared.models import EtlBadRow, EtlJobRun
from shared.visao_cliente_schema import REQUIRED_COLUMNS, normalize_column_name
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done
from worker.steps.extract import get_cached_dataframe

_ALLOWED_NIVEL_CARTAO = {"sem_cartao", "baixo", "medio", "alto"}
_ALLOWED_NIVEL_CONTA = {"sem_conta", "baixo", "medio", "alto"}


def _missing_required_columns(columns: list[str]) -> list[str]:
    if not REQUIRED_COLUMNS:
        return []
    normalized = {normalize_column_name(c) for c in columns}
    return [col for col in REQUIRED_COLUMNS if col.lower() not in normalized]


def _normalize_level_value(value) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return None

    normalized = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()

    aliases = {
        "sem cartao": "sem_cartao",
        "sem conta": "sem_conta",
        "medio": "medio",
        "baixo": "baixo",
        "alto": "alto",
    }
    return aliases.get(normalized, normalized.replace(" ", "_"))


def run_validate(session: Session, job_id: str, etl_file) -> None:
    if is_step_done(session, job_id, "validate"):
        return
    begin_step(session, job_id, "validate")

    settings = get_settings()
    dataframe = get_cached_dataframe(job_id)
    if dataframe is None:
        raise RuntimeError("No dataframe in cache, extract must run first")

    missing_columns = _missing_required_columns(list(dataframe.columns))
    if missing_columns:
        raise ValueError(f"Schema validation failed, missing columns: {missing_columns}")

    bad_rows: list[EtlBadRow] = []

    # Linhas totalmente nulas
    all_null_mask = dataframe.isnull().all(axis=1)
    for idx in dataframe.index[all_null_mask]:
        bad_rows.append(
            EtlBadRow(
                id=str(uuid.uuid4()),
                job_id=job_id,
                row_number=int(idx),
                raw_data=dataframe.loc[idx].to_dict(),
                reason="all_null_row",
            )
        )

    # Validacao vetorizada de nivel_cartao / nivel_conta (exclui linhas all-null)
    non_null = dataframe[~all_null_mask].copy()
    if not non_null.empty:
        norm_cartao = non_null["nivel_cartao"].map(_normalize_level_value)
        norm_conta = non_null["nivel_conta"].map(_normalize_level_value)

        invalid_cartao = norm_cartao.notna() & ~norm_cartao.isin(_ALLOWED_NIVEL_CARTAO)
        invalid_conta = norm_conta.notna() & ~norm_conta.isin(_ALLOWED_NIVEL_CONTA)
        invalid_mask = invalid_cartao | invalid_conta

        for idx in non_null.index[invalid_mask]:
            fields: list[str] = []
            if invalid_cartao.loc[idx]:
                fields.append(f"nivel_cartao={non_null.loc[idx, 'nivel_cartao']!r}")
            if invalid_conta.loc[idx]:
                fields.append(f"nivel_conta={non_null.loc[idx, 'nivel_conta']!r}")
            bad_rows.append(
                EtlBadRow(
                    id=str(uuid.uuid4()),
                    job_id=job_id,
                    row_number=int(idx),
                    raw_data=non_null.loc[idx].to_dict(),
                    reason=f"invalid_level_value: {'; '.join(fields)}",
                )
            )

    total = len(dataframe)
    bad_count = len(bad_rows)

    # Bulk insert em vez de N roundtrips individuais (F02 — QA fix)
    if bad_rows:
        session.bulk_save_objects(bad_rows)

    job = session.query(EtlJobRun).filter_by(id=job_id).first()
    if job is not None:
        job.rows_total = total
        job.rows_bad = bad_count
        job.rows_ok = total - bad_count

    session.flush()

    if total > 0 and (bad_count / total * 100) > settings.BAD_ROW_THRESHOLD_PCT:
        raise ValueError(
            f"Bad row threshold exceeded: {bad_count}/{total} "
            f"({(bad_count / total) * 100:.1f}%) > {settings.BAD_ROW_THRESHOLD_PCT}%"
        )

    mark_step_done(session, job_id, "validate")
