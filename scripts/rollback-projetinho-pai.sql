-- =============================================================
-- ROLLBACK: Restaura projetinho_pai ao estado antes do último ETL
--
-- Quando usar:
--   Depois de um ETL que deu errado e precisa voltar o banco.
--
-- Como usar:
--   1. Descubra o nome do backup mais recente:
--        SELECT tablename FROM pg_tables
--        WHERE schemaname = 'public' AND tablename LIKE 'projetinho_pai_backup_%'
--        ORDER BY tablename DESC LIMIT 5;
--
--   2. Troque a data abaixo pela data do backup desejado e execute:
-- =============================================================

-- Ajuste a data (YYYYMMDD) para o backup que deseja restaurar:
-- Exemplo: projetinho_pai_backup_20260406

BEGIN;

DELETE FROM public.projetinho_pai;
INSERT INTO public.projetinho_pai SELECT * FROM public.projetinho_pai_backup_20260406;

-- Confirma quantas linhas foram restauradas:
SELECT COUNT(*) AS linhas_restauradas FROM public.projetinho_pai;

COMMIT;

-- =============================================================
-- VERIFICAÇÃO PÓS-ROLLBACK
-- Execute após o rollback para confirmar que está tudo ok:
-- =============================================================

-- Checa FK integrity (deve retornar 0):
SELECT COUNT(*) AS orfas
FROM public.lead_atribuicoes la
WHERE NOT EXISTS (
    SELECT 1 FROM public.projetinho_pai pp WHERE pp.id = la.lead_id
);

-- Lista os backups disponíveis:
SELECT tablename, pg_size_pretty(pg_total_relation_size('public.' || tablename)) AS tamanho
FROM pg_tables
WHERE schemaname = 'public' AND tablename LIKE 'projetinho_pai_backup_%'
ORDER BY tablename DESC;
