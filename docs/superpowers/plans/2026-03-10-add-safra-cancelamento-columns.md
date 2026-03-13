# Add Safra/Cancelamento Columns Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 6 new columns from the Excel sheet to `staging_visao_cliente` and `final_visao_cliente` so the ETL pipeline captures them end-to-end.

**Architecture:** All 6 columns are pass-through (read from Excel, stored as Text, no enrichment logic). Changes are confined to: the schema constant list, the Alembic migration, and the test fixture. The `clean`, `stage`, and `upsert` steps already handle columns dynamically and require no changes.

**Tech Stack:** Python, SQLAlchemy, Alembic, openpyxl, pytest

---

## New columns

| Excel header | Normalized name | Type |
|---|---|---|
| `CANCELAMENTO_MAQ` | `cancelamento_maq` | Text |
| `ELEGIVEL_C6` | `elegivel_c6` | Text |
| `SAFRA_BOLETO` | `safra_boleto` | Text |
| `IDADE_SAFRA_BOLETO` | `idade_safra_boleto` | Text |
| `SAFRA_MAQUINA` | `safra_maquina` | Text |
| `IDADE_SAFRA_MAQUINA` | `idade_safra_maquina` | Text |

## Files

- Modify: `shared/visao_cliente_schema.py` — append 6 names to `REQUIRED_COLUMNS`
- Create: `migrations/versions/20260310_000009_add_safra_cancelamento_columns.py` — ADD COLUMN migration
- Modify: `tests/fixtures/make_xlsx.py` — add 6 headers + row values to fixture
- Modify: `tests/unit/test_etl_steps.py` — add assertion that schema has the 6 new columns

---

## Chunk 1: Schema + Migration + Fixture + Tests

### Task 1: Add columns to REQUIRED_COLUMNS

**Files:**
- Modify: `shared/visao_cliente_schema.py` (after line 115, `"nivel_conta"`)

- [ ] **Step 1: Add 6 new entries to end of REQUIRED_COLUMNS**

  In `shared/visao_cliente_schema.py`, after `"nivel_conta",` add:
  ```python
      "cancelamento_maq",
      "elegivel_c6",
      "safra_boleto",
      "idade_safra_boleto",
      "safra_maquina",
      "idade_safra_maquina",
  ```

- [ ] **Step 2: Verify length**

  Run: `python -c "from shared.visao_cliente_schema import REQUIRED_COLUMNS; print(len(REQUIRED_COLUMNS))"`
  Expected: `111`

---

### Task 2: Create Alembic migration

**Files:**
- Create: `migrations/versions/20260310_000009_add_safra_cancelamento_columns.py`

- [ ] **Step 1: Create migration file**

  ```python
  """add safra and cancelamento columns to visao cliente tables

  Revision ID: 20260310_000009
  Revises: 20260302_000008
  Create Date: 2026-03-10 00:00:00
  """

  from typing import Sequence, Union
  from alembic import op
  import sqlalchemy as sa

  revision: str = "20260310_000009"
  down_revision: Union[str, None] = "20260302_000008"
  branch_labels: Union[str, Sequence[str], None] = None
  depends_on: Union[str, Sequence[str], None] = None

  NEW_COLS = [
      "cancelamento_maq",
      "elegivel_c6",
      "safra_boleto",
      "idade_safra_boleto",
      "safra_maquina",
      "idade_safra_maquina",
  ]


  def upgrade() -> None:
      for col in NEW_COLS:
          op.add_column("staging_visao_cliente", sa.Column(col, sa.Text(), nullable=True))
      for col in NEW_COLS:
          op.add_column("final_visao_cliente", sa.Column(col, sa.Text(), nullable=True))


  def downgrade() -> None:
      for col in reversed(NEW_COLS):
          op.drop_column("final_visao_cliente", col)
      for col in reversed(NEW_COLS):
          op.drop_column("staging_visao_cliente", col)
  ```

---

### Task 3: Update test fixture

**Files:**
- Modify: `tests/fixtures/make_xlsx.py`

- [ ] **Step 1: Add 6 headers to VISAO_CLIENTE_HEADERS**

  Append after `"nivel_conta"` in the `VISAO_CLIENTE_HEADERS` list:
  ```python
  "cancelamento_maq", "elegivel_c6",
  "safra_boleto", "idade_safra_boleto",
  "safra_maquina", "idade_safra_maquina",
  ```

- [ ] **Step 2: _row_values already handles unknown cols with `else: row.append(None)` — no change needed.**

---

### Task 4: Add unit test for new columns

**Files:**
- Modify: `tests/unit/test_etl_steps.py`

- [ ] **Step 1: Add test**

  ```python
  def test_required_columns_include_safra_and_cancelamento():
      from shared.visao_cliente_schema import REQUIRED_COLUMNS
      for col in ("cancelamento_maq", "elegivel_c6", "safra_boleto",
                  "idade_safra_boleto", "safra_maquina", "idade_safra_maquina"):
          assert col in REQUIRED_COLUMNS, f"Missing column: {col}"
  ```

- [ ] **Step 2: Run unit tests**

  Run: `cd /c/Users/MB\ NEGOCIOS/etl-system && python -m pytest tests/unit/ -v -x 2>&1 | tail -30`
  Expected: all PASS

- [ ] **Step 3: Commit**

  ```bash
  git add shared/visao_cliente_schema.py \
          migrations/versions/20260310_000009_add_safra_cancelamento_columns.py \
          tests/fixtures/make_xlsx.py \
          tests/unit/test_etl_steps.py
  git commit -m "feat: add safra/cancelamento/elegivel columns to visao cliente pipeline"
  ```
