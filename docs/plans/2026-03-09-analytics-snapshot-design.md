# Analytics Snapshot — Design

**Data:** 2026-03-09

## Regras de negócio confirmadas pelo usuário

Todos os 4 indicadores são calculados a partir da aba "Visão Cliente" do arquivo "Relatório de Produção".

### 1. contas-abertas
- `TIPO_PESSOA = PJ`
- `STATUS_CC = LIBERADA`
- `DT_CONTA_CRIADA` no mês de referência do arquivo
- **Fórmula:** COUNT

### 2. contas-qualificadas
- `FL_QUALIFICADO_COMISS = 1`
- **Fórmula:** COUNT

### 3. instalacao-c6pay
- `TIPO_PESSOA = PJ`
- `STATUS_CC = LIBERADA`
- `DT_INSTALL_MAQ` no mês de referência do arquivo
- **Fórmula:** COUNT

### 4. qualificacao-c6pay
- `TIPO_PESSOA = PJ`
- `STATUS_CC = LIBERADA`
- `DT_INSTALL_MAQ >= reference_date - 90 dias`
- `DT_CANCELAMENTO_MAQ` vazia (NULL)
- `TPV_M0 >= 5000`
- **Fórmula:** COUNT

## Notas
- A janela de 90 dias usa `reference_date` (data do arquivo), não `date.today()`
- Colunas "Total de Contas Abertas", "Contas Qualificadas", "Maquinas Vendidas Relacionamento" não existem no arquivo — foram descartadas
