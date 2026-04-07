# Enrich Fix + Script Processador de Relatório — Plano de Implementação

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Corrigir bugs de acentuação no enrich.py e gerar `Relatório de Produção - 02.02.26 (processado).xlsx` com 25 colunas calculadas.

**Architecture:** (1) Corrige `worker/steps/enrich.py` para usar "Não"/"NÃO" e status com acentos corretos. (2) Script standalone `scripts/processar_relatorio.py` reutiliza as funções já corrigidas do enrich, normaliza colunas na entrada e saída, e salva o xlsx processado.

**Tech Stack:** Python 3.13, pandas, openpyxl, numpy — sem Docker necessário.

---

### Task 1: Corrigir bugs de acentuação e status em enrich.py

**Files:**
- Modify: `worker/steps/enrich.py:179-202`

**Contexto:**
O `_compute_status_columns` usa "Nao"/"NAO" (sem acento) e os status B/C/E sem acento.
O docx e o arquivo de referência usam "Não"/"NÃO" e "qualificação" com acento.

**Step 1: Abrir o arquivo e localizar a função**

Arquivo: `worker/steps/enrich.py`, função `_compute_status_columns` (linha ~173).
Buscar todas as ocorrências de: `"Nao"`, `"NAO"`, `"qualificacao"`, `"qualificacoes"`.

**Step 2: Aplicar as correções**

Substituir em `_compute_status_columns`:

```python
# ANTES:
ja_recebeu = np.where(ja_pago > 0, "SIM", "Nao")
# Excel: =IF([@PREVISAO_COMISS]>0,"SIM","NAO")  — "NAO" tudo maiusculo
prox_mes = np.where(previsao > 0, "SIM", "NAO")

# DEPOIS:
ja_recebeu = np.where(ja_pago > 0, "SIM", "Não")
# Excel: =IF([@PREVISAO_COMISS]>0,"SIM","NÃO")
prox_mes = np.where(previsao > 0, "SIM", "NÃO")
```

Substituir as comparações internas:

```python
# ANTES:
status = np.where(
    (ja_recebeu == "Nao") & (prox_mes == "NAO"),
    "A - Nunca qualificou",
    np.where(
        (ja_recebeu == "Nao") & (prox_mes == "SIM"),
        "B - Primeira qualificacao",
        np.where(
            (ja_recebeu == "SIM") & (prox_mes == "SIM"),
            "C - Qualificacao recorrente",
            np.where(is_max, "D - Topo atingido", "E - Perdeu qualificacao"),
        ),
    ),
)

# DEPOIS:
status = np.where(
    (ja_recebeu == "Não") & (prox_mes == "NÃO"),
    "A - Nunca qualificou",
    np.where(
        (ja_recebeu == "Não") & (prox_mes == "SIM"),
        "B - Primeira qualificação",
        np.where(
            (ja_recebeu == "SIM") & (prox_mes == "SIM"),
            "C - Qualificação recorrente",
            np.where(is_max, "D - Topo atingido", "E - Perdeu qualificação"),
        ),
    ),
)
```

Também atualizar o comentário da linha 179:
```python
# Excel: =IF([@JA_PAGO_COMISS]>0,"SIM","Não")
```

**Step 3: Atualizar o teste que quebra**

Arquivo: `tests/unit/test_etl_steps.py`, linha 126.

```python
# ANTES:
assert row["comissao_prox_mes"] == "NAO"

# DEPOIS:
assert row["comissao_prox_mes"] == "NÃO"
```

O status "D - Topo atingido" (linha 127) não muda, mas o teste checa `ja_recebeu_comissao == "SIM"` — isso continua correto.

**Step 4: Rodar os testes unitários**

```bash
cd C:\Users\MB NEGOCIOS\etl-system\.worktrees\implementation
pytest tests/unit/test_etl_steps.py -v
```

Esperado: todos PASS.

**Step 5: Commit**

```bash
git add worker/steps/enrich.py tests/unit/test_etl_steps.py
git commit -m "fix: corrige acentuacao em JA_RECEBEU_COMISSAO, COMISSAO_PROX_MES e STATUS_QUALIFICACAO"
```

---

### Task 2: Criar o script standalone processar_relatorio.py

**Files:**
- Create: `scripts/processar_relatorio.py`

**Contexto:**
O script lê um xlsx C6 Bank, normaliza colunas para lowercase, reutiliza as funções
de enrich.py, e salva o xlsx com 25 colunas adicionadas.

Mapeamento de nomes de saída (lowercase interno → UPPERCASE no xlsx):

```python
OUTPUT_RENAME = {
    "faixa_max":           "FAIXA_MAX",
    "faixa_alvo":          "FAIXA_ALVO",
    "threshiold_cash_in":  "THRESHIOLD_CASH_IN",   # typo preservado da planilha original
    "threshold_spending":  "THRESHOLD_SPENDING",
    "threshold_saldo_medio": "THRESHOLD_SALDO_MEDIO",
    "threshold_conta_global": "THRESHOLD_CONTA_GLOBAL",
    "threshold_domicilio": "THRESHOLD_DOMICILIO",
    "gap_cash_in":         "GAP_CASH_IN",
    "gap_spending":        "GAP_SPENDING",
    "gap_saldo_medio":     "GAP_SALDO_MEDIO",
    "gap_conta_global":    "GAP_CONTA_GLOBAL",
    "gap_domicilio":       "GAP_DOMICILIO",
    "pct_cash_in":         "%_CASH_IN",
    "pct_spending":        "%_SPENDING",
    "pct_saldo_medio":     "%_SALDO_MEDIO",
    "pct_conta_global":    "%_CONTA_GLOBAL",
    "maior_progresso_pct": "MAIOR_PROGRESSO%",
    "criterio_proximo":    "CRITERIO_PROXIMO",
    "ja_recebeu_comissao": "JA_RECEBEU_COMISSAO",
    "comissao_prox_mes":   "COMISSAO_PROX_MES",
    "status_qualificacao": "STATUS_QUALIFICAÇÃO",
    "dias_desde_abertura": "DIAS_DESDE_ABERTURA",
    "m2_dias_faltantes":   "M2_DIAS_FALTANTES",
    "nivel_cartao":        "NIVEL_CARTAO",
    "nivel_conta":         "NIVEL_CONTA",
}
```

**Step 1: Criar a pasta scripts se não existir e criar o arquivo**

```bash
mkdir -p C:\Users\MB NEGOCIOS\etl-system\.worktrees\implementation\scripts
```

Criar `scripts/processar_relatorio.py` com o seguinte conteúdo:

```python
"""
Script standalone: adiciona colunas calculadas a um Relatorio de Producao C6 Bank.

Uso:
    python scripts/processar_relatorio.py <caminho_do_xlsx>

Saída:
    <mesmo_diretorio>/<nome_original> (processado).xlsx
"""
import sys
import os
from pathlib import Path

import pandas as pd
import numpy as np

# Adiciona raiz do projeto ao path para importar worker/shared
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.visao_cliente_schema import normalize_column_name
from worker.steps.enrich import (
    _coerce_numeric,
    _coerce_datetime,
    _compute_levels,
    _compute_gap_columns,
    _compute_status_columns,
    _compute_day_metrics,
)

SHEET_NAME = "Visão Cliente"

OUTPUT_RENAME = {
    "faixa_max":              "FAIXA_MAX",
    "faixa_alvo":             "FAIXA_ALVO",
    "threshiold_cash_in":     "THRESHIOLD_CASH_IN",
    "threshold_spending":     "THRESHOLD_SPENDING",
    "threshold_saldo_medio":  "THRESHOLD_SALDO_MEDIO",
    "threshold_conta_global": "THRESHOLD_CONTA_GLOBAL",
    "threshold_domicilio":    "THRESHOLD_DOMICILIO",
    "gap_cash_in":            "GAP_CASH_IN",
    "gap_spending":           "GAP_SPENDING",
    "gap_saldo_medio":        "GAP_SALDO_MEDIO",
    "gap_conta_global":       "GAP_CONTA_GLOBAL",
    "gap_domicilio":          "GAP_DOMICILIO",
    "pct_cash_in":            "%_CASH_IN",
    "pct_spending":           "%_SPENDING",
    "pct_saldo_medio":        "%_SALDO_MEDIO",
    "pct_conta_global":       "%_CONTA_GLOBAL",
    "maior_progresso_pct":    "MAIOR_PROGRESSO%",
    "criterio_proximo":       "CRITERIO_PROXIMO",
    "ja_recebeu_comissao":    "JA_RECEBEU_COMISSAO",
    "comissao_prox_mes":      "COMISSAO_PROX_MES",
    "status_qualificacao":    "STATUS_QUALIFICAÇÃO",
    "dias_desde_abertura":    "DIAS_DESDE_ABERTURA",
    "m2_dias_faltantes":      "M2_DIAS_FALTANTES",
    "nivel_cartao":           "NIVEL_CARTAO",
    "nivel_conta":            "NIVEL_CONTA",
}

# Colunas que já existiam no xlsx (para preservar nome original UPPERCASE)
ORIGINAL_COLS_UPPERCASE = [
    "DATA_BASE", "CD_CPF_CNPJ_CLIENTE", "NOME_CLIENTE", "TIPO_PESSOA",
    "CD_CPF_CNPJ_PARCEIRO", "NOME_PARCEIRO", "CD_CPF_CNPJ_CONSULTOR", "NOME_CONSULTOR",
    "UF", "CIDADE", "BAIRRO", "TELEFONE", "TELEFONE_MASTER", "EMAIL",
    "DT_FUNDACAO_EMPRESA", "RAMO_ATUACAO", "NUM_CONTA", "LIMITE_CONTA",
    "DT_CONTA_CRIADA", "DT_ENCER_CC", "STATUS_CC", "CONTA_ATIVA_90D",
    "CHAVES_PIX_FORTE", "VL_CASH_IN_MTD", "LIMITE_CARTAO", "LIMITE_ALOCADO_CARTAO_CDB",
    "DT_ENTREGA_CARTAO", "DT_ATIV_CARTAO_CRED", "VL_SPENDING_TOTAL_MTD",
    "STATUS_PAGAMENTO_FATURA", "FL_PROPENSAO_C6PAY", "TPV_C6PAY_POTENCIAL",
    "FL_ELEGIVEL_VENDA_C6PAY", "STATUS_PROPOSTA_SF_PAY", "DT_APROVACAO_PAY",
    "DT_INSTALL_MAQ", "DT_ATIVACAO_PAY", "C6PAY_ATIVA_30", "DT_CANCELAMENTO_MAQ",
    "DT_ULT_TRANS_PAY", "RECEBIMENTO", "BANCO_DOMICILIO", "TPV_M2", "TPV_M1", "TPV_M0",
    "FAIXA_TPV_PROMETIDO", "FL_PROPENSAO_BOLCOB", "TPV_BOLCOB_POTENCIAL",
    "FL_BOLCOB_CADASTRADO", "DT_PRIM_LIQ_BOLCOB", "DT_ULT_EMISSAO_BOLCOB",
    "QTD_BOLCOB_EMTD_MTD", "VL_BOLCOB_EMTD_MTD", "QTD_BOLCOB_LIQ_MTD",
    "VL_BOLCOB_LIQ_MTD", "VOLUME_ANTECIPADO", "AGENDA_DISPONIVEL", "TAXA_ANTECIPACAO",
    "VL_SALDO_MEDIO_MENSALIZADO", "DT_CONTA_CRIADA_GLOBAL", "VL_CASH_IN_CONTA_GLOBAL_MTD",
    "FL_CASH_IN_PURO", "FL_CASH_IN_BOLETO", "FL_CASH_IN_SETUP", "FL_CASH_IN_SETUP_PIX_CNPJ",
    "FL_CASH_IN_SETUP_CDB_CARTAO", "FL_CASH_IN_SETUP_PAGAMENTOS", "FL_CASH_IN_SETUP_DEB_AUTO",
    "MES_REF_COMISS", "FL_QUALIFICADO_COMISS", "FAIXA_CASH_IN", "FAIXA_DOMICILIO",
    "FAIXA_SALDO_MEDIO", "FAIXA_SPENDING", "FAIXA_CASH_IN_GLOBAL",
    "CRITERIOS_ATINGIDOS_COMISS", "APURACAO_COMISS", "MULTIPLICADOR",
    "JA_PAGO_COMISS", "PREVISAO_COMISS",
]


def _normalize_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Normaliza colunas para lowercase e retorna mapeamento original->normalizado."""
    mapping = {}
    new_cols = []
    for col in df.columns:
        norm = normalize_column_name(col)
        mapping[norm] = col
        new_cols.append(norm)
    df.columns = new_cols
    return df, mapping


def _restore_original_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia colunas: originais voltam para UPPERCASE, calculadas recebem nome padrão."""
    rename_map = {}
    for col in df.columns:
        if col in OUTPUT_RENAME:
            rename_map[col] = OUTPUT_RENAME[col]
        else:
            # Tenta restaurar o nome UPPERCASE original
            upper = col.upper()
            if upper in ORIGINAL_COLS_UPPERCASE:
                rename_map[col] = upper
    return df.rename(columns=rename_map)


def processar(input_path: str) -> str:
    input_path = Path(input_path)
    print(f"Lendo: {input_path.name}")

    df = pd.read_excel(input_path, sheet_name=SHEET_NAME)
    print(f"  {len(df):,} linhas x {len(df.columns)} colunas originais")

    # Garante que colunas calculadas existentes sejam removidas para recalcular
    cols_to_drop = [c for c in df.columns if c in list(OUTPUT_RENAME.values())]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
        print(f"  Removendo {len(cols_to_drop)} colunas calculadas existentes para recalcular")

    df, _col_map = _normalize_columns(df)

    # Garante colunas mínimas necessárias
    required_input = [
        "faixa_cash_in", "faixa_domicilio", "faixa_saldo_medio",
        "faixa_spending", "faixa_cash_in_global",
        "vl_cash_in_mtd", "vl_spending_total_mtd",
        "vl_saldo_medio_mensalizado", "vl_cash_in_conta_global_mtd",
        "ja_pago_comiss", "previsao_comiss",
        "limite_cartao", "limite_conta",
        "data_base", "dt_conta_criada",
    ]
    missing = [c for c in required_input if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}")

    print("  Calculando colunas derivadas...")
    _compute_levels(df)
    _compute_gap_columns(df)
    _compute_status_columns(df)
    _compute_day_metrics(df)

    df = _restore_original_columns(df)

    added = [c for c in df.columns if c in list(OUTPUT_RENAME.values())]
    print(f"  {len(added)} colunas adicionadas: {added}")
    print(f"  Total: {len(df.columns)} colunas")

    stem = input_path.stem
    output_path = input_path.parent / f"{stem} (processado).xlsx"
    print(f"Salvando: {output_path.name}")
    df.to_excel(output_path, sheet_name=SHEET_NAME, index=False)
    print(f"Concluído: {output_path}")
    return str(output_path)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python scripts/processar_relatorio.py <caminho_do_xlsx>")
        sys.exit(1)
    processar(sys.argv[1])
```

**Step 2: Verificar que o script importa corretamente (dry-run)**

```bash
cd C:\Users\MB NEGOCIOS\etl-system\.worktrees\implementation
python -c "import scripts.processar_relatorio; print('OK')"
```

Esperado: `OK` sem erros.

**Step 3: Commit do script**

```bash
git add scripts/processar_relatorio.py
git commit -m "feat: adiciona script standalone para processar relatorio C6 Bank"
```

---

### Task 3: Executar o script no arquivo 02.02.26

**Files:**
- Input: `C:\Users\MB NEGOCIOS\Downloads\Relatório de Produção - 02.02.26.xlsx`
- Output: `C:\Users\MB NEGOCIOS\Downloads\Relatório de Produção - 02.02.26 (processado).xlsx`

**Step 1: Rodar o script**

```bash
cd C:\Users\MB NEGOCIOS\etl-system\.worktrees\implementation
python scripts/processar_relatorio.py "C:\Users\MB NEGOCIOS\Downloads\Relatório de Produção - 02.02.26.xlsx"
```

Esperado:
```
Lendo: Relatório de Produção - 02.02.26.xlsx
  157246 linhas x 80 colunas originais
  Calculando colunas derivadas...
  25 colunas adicionadas: [...]
  Total: 105 colunas
Salvando: Relatório de Produção - 02.02.26 (processado).xlsx
Concluído: ...
```

**Step 2: Validar contra o arquivo de referência 21.02.26**

Rodar script de validação inline:

```bash
python << 'EOF'
import pandas as pd, sys, os
sys.stdout.reconfigure(encoding='utf-8')
downloads = r'C:\Users\MB NEGOCIOS\Downloads'

df_proc = pd.read_excel(
    os.path.join(downloads, 'Relatório de Produção - 02.02.26 (processado).xlsx'),
    sheet_name='Visão Cliente', nrows=100
)
df_ref = pd.read_excel(
    os.path.join(downloads, 'Relatório de Produção - 21.02.26.xlsx'),
    sheet_name='Visão Cliente', nrows=100
)

CHECK_COLS = [
    'FAIXA_MAX', 'FAIXA_ALVO', 'THRESHIOLD_CASH_IN',
    'GAP_CASH_IN', '%_CASH_IN', 'MAIOR_PROGRESSO%',
    'CRITERIO_PROXIMO', 'JA_RECEBEU_COMISSAO', 'COMISSAO_PROX_MES',
    'STATUS_QUALIFICAÇÃO', 'NIVEL_CARTAO', 'NIVEL_CONTA',
]

print('=== COLUNAS PRESENTES NO PROCESSADO ===')
for col in CHECK_COLS:
    presente = col in df_proc.columns
    print(f'  {col}: {"OK" if presente else "AUSENTE"}')

print('\n=== VALORES UNICOS JA_RECEBEU_COMISSAO ===')
print(df_proc['JA_RECEBEU_COMISSAO'].value_counts().to_dict())

print('\n=== VALORES UNICOS COMISSAO_PROX_MES ===')
print(df_proc['COMISSAO_PROX_MES'].value_counts().to_dict())

print('\n=== VALORES UNICOS STATUS_QUALIFICAÇÃO ===')
print(df_proc['STATUS_QUALIFICAÇÃO'].value_counts().to_dict())

print('\n=== COMPARACAO FAIXA_MAX (referencia vs processado) ===')
ref_vals = df_ref['FAIXA_MAX'].value_counts().to_dict()
proc_vals = df_proc['FAIXA_MAX'].value_counts().to_dict()
print(f'  Ref:  {ref_vals}')
print(f'  Proc: {proc_vals}')
EOF
```

Esperado:
- Todas as 25 colunas presentes
- `JA_RECEBEU_COMISSAO` com valores `"SIM"` e `"Não"` (com acento)
- `COMISSAO_PROX_MES` com valores `"SIM"` e `"NÃO"` (com acento)
- `STATUS_QUALIFICAÇÃO` com os 5 status com acentos corretos
- `FAIXA_MAX` com distribuição razoável (maioria 0)

---

### Task 4: Validação final dos testes unitários

**Step 1: Rodar todos os testes unitários**

```bash
cd C:\Users\MB NEGOCIOS\etl-system\.worktrees\implementation
pytest tests/unit/ -v
```

Esperado: todos PASS.

**Step 2: Commit final se tudo OK**

```bash
git add .
git commit -m "chore: valida script e testes apos correcao enrich"
```
