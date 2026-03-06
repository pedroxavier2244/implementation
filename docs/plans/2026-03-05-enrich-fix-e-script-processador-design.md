# Design: Correção do enrich.py + Script Processador de Relatório

**Data:** 2026-03-05

## Contexto

O ETL processa arquivos Excel do C6 Bank (sheet "Visão Cliente") e adiciona 25 colunas
calculadas com base nas regras de negócio definidas em REGRAS_DE_NEGOCIO_C6_BANK.

O arquivo `Relatório de Produção - 02.02.26.xlsx` possui 80 colunas (A-CB, dados originais)
e precisa receber as 25 colunas calculadas para ficar no padrão do arquivo de referência
`Relatório de Produção - 21.02.26.xlsx` (103 colunas).

## Problema

### Bug 1: Acentuação incorreta em JA_RECEBEU_COMISSAO
- **enrich.py atual:** `"Nao"` (sem acento)
- **Correto (docx + referência):** `"Não"` (com til)

### Bug 2: Acentuação incorreta em COMISSAO_PROX_MES
- **enrich.py atual:** `"NAO"` (sem acento)
- **Correto (docx + referência):** `"NÃO"` (com til)

Impacto secundário: `_compute_status_columns` compara internamente com "Nao"/"NAO",
então precisa ser atualizado junto.

## Colunas adicionadas (25 total)

| Coluna | Lógica |
|---|---|
| FAIXA_MAX | MAX das 5 faixas (0-4) |
| FAIXA_ALVO | FAIXA_MAX+1, ou "MAX" se >=4 |
| THRESHIOLD_CASH_IN | Meta Cash In por faixa alvo (6k/15k/30k/50k) |
| THRESHOLD_SPENDING | Meta Spending (5k/8k/11k/15k) |
| THRESHOLD_SALDO_MEDIO | Meta Saldo Médio (1k/2k/4k/8k) |
| THRESHOLD_CONTA_GLOBAL | Meta Cash In Global (5k/10k/20k/30k) |
| THRESHOLD_DOMICILIO | Meta Domicílio (5k/12k/18k/25k) |
| GAP_CASH_IN | Quanto falta de Cash In para meta |
| GAP_SPENDING | Quanto falta de Spending para meta |
| GAP_SALDO_MEDIO | Quanto falta de Saldo Médio para meta |
| GAP_CONTA_GLOBAL | Quanto falta de Cash In Global para meta |
| GAP_DOMICILIO | "SEM DADOS" se não atingiu faixa |
| %_CASH_IN | Progresso % do Cash In em relação à meta |
| %_SPENDING | Progresso % do Spending |
| %_SALDO_MEDIO | Progresso % do Saldo Médio |
| %_CONTA_GLOBAL | Progresso % da Conta Global |
| MAIOR_PROGRESSO% | Maior % entre os 4 critérios (ignora >=100%) |
| CRITERIO_PROXIMO | Critério com maior progresso (mais fácil de subir) |
| JA_RECEBEU_COMISSAO | "SIM"/"Não" baseado em JA_PAGO_COMISS |
| COMISSAO_PROX_MES | "SIM"/"NÃO" baseado em PREVISAO_COMISS |
| STATUS_QUALIFICAÇÃO | A/B/C/D/E baseado nos flags acima + FAIXA_ALVO |
| DIAS_DESDE_ABERTURA | DATA_BASE - DT_CONTA_CRIADA (dias), "SEM DADOS" se vazio |
| M2_DIAS_FALTANTES | DIAS_DESDE_ABERTURA - 60 |
| NIVEL_CARTAO | Classificação do limite de cartão (Sem Cartao/Baixo/Medio/Alto) |
| NIVEL_CONTA | Classificação do limite de conta (Sem Conta/Baixo/Medio/Alto) |

## Solução

### Parte 1: Correção do enrich.py

Arquivo: `worker/steps/enrich.py`

- `_compute_status_columns`: substituir `"Nao"` → `"Não"` e `"NAO"` → `"NÃO"`
  em todas as ocorrências (produção dos valores E comparações internas)

### Parte 2: Script standalone

Arquivo: `scripts/processar_relatorio.py`

O script reutiliza as funções de `enrich.py` diretamente:
1. Lê o xlsx de entrada (argumento CLI ou caminho fixo)
2. Normaliza nomes de colunas para lowercase (padrão do enrich.py)
3. Executa as funções `_compute_levels`, `_compute_gap_columns`,
   `_compute_status_columns`, `_compute_day_metrics`
4. Renomeia colunas de saída para UPPERCASE do padrão C6 Bank
5. Salva `<nome_original> (processado).xlsx` na mesma pasta

### Validação

Após gerar o arquivo, o script compara amostras das colunas calculadas
com o arquivo de referência 21.02.26 para confirmar consistência.

## Arquivos modificados

- `worker/steps/enrich.py` — corrigir 2 bugs de acentuação
- `scripts/processar_relatorio.py` — novo script standalone
- `tests/unit/test_etl_steps.py` — adicionar testes para valores com acento
