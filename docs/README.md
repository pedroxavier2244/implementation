# Documentacao

> Status: current
> Last validated against code: 2026-03-26

Leia primeiro: [../AI-START-HERE.md](../AI-START-HERE.md)

## Ordem recomendada de leitura

1. [AI START HERE](../AI-START-HERE.md)
2. [Guia de Integracao do ETL](guia-integracao-etl.md) se o objetivo for consumir a API ou operar jobs
3. [Guia de Manutencao do ETL](manutencao-etl.md) se o objetivo for deploy, infra ou troubleshooting
4. documentos legados apenas quando voce precisar de contexto historico

## Mapa de status dos documentos

| Documento | Status | Quando ler | Observacao |
| --- | --- | --- | --- |
| `guia-integracao-etl.md` | current | consumo da API e operacao manual | guia principal para integracao |
| `manutencao-etl.md` | current | deploy e sustentacao | guia principal de operacao |
| `fluxo-etl.md` | legacy | contexto historico | descreve uma arquitetura mais ampla que o wiring atual |
| `api-integracao.md` | legacy | contrato historico | nao e a melhor fonte para comportamento atual |
| `architecture/` | legacy | historico de planos e specs de design | nao ler primeiro |

## Regra rapida para outra IA

- use `AI-START-HERE.md` para onboarding curto
- valide comportamento no codigo quando houver conflito
- trate `fluxo-etl.md` e `api-integracao.md` como historico, nao como fonte principal de verdade