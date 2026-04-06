#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dump do banco de produção via VPS (que tem acesso ao Supabase).
Roda pg_dump no VPS e baixa o arquivo via SFTP para tmp/prod_dump.dump.
"""
import io
import os
import sys

# Força UTF-8 no stdout (Windows usa cp1252 por padrão)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

try:
    import paramiko
except ImportError:
    print("Instalando paramiko...")
    os.system(f"{sys.executable} -m pip install paramiko -q")
    import paramiko

VPS_HOST = "5.189.163.33"
VPS_PORT = 22
VPS_USER = "root"
VPS_PASS = "cb5D75sc41Txr"

PROD_HOST = "db.qdkiksitojyeembgzdey.supabase.co"
PROD_USER = "postgres"
PROD_PASS = "Mbfinance@2026"
PROD_DB   = "postgres"

REMOTE_DUMP = "/tmp/hml_prod_dump.dump"
# Sempre salva relativo à raiz do projeto (um nível acima de scripts/)
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
LOCAL_DUMP  = os.path.join(PROJECT_DIR, "tmp", "prod_dump.dump")

os.makedirs(os.path.join(PROJECT_DIR, "tmp"), exist_ok=True)

print(f"[1/3] Conectando na VPS {VPS_HOST}...")
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(VPS_HOST, port=VPS_PORT, username=VPS_USER, password=VPS_PASS, timeout=30)
print("      OK Conectado")

print("[2/3] Rodando pg_dump no VPS via Docker postgres:17 (pode demorar alguns minutos)...")
# pg_dump 16 instalado na VPS nao consegue fazer dump do PostgreSQL 17 (Supabase).
# Solucao: rodar postgres:17-alpine com --network container:implementation-worker-etl-1
# para herdar o namespace de rede do container ETL (que ja tem acesso ao Supabase).
cmd = (
    f"docker run --rm "
    f"--network container:implementation-worker-etl-1 "
    f"-e PGPASSWORD='{PROD_PASS}' "
    f"postgres:17-alpine "
    f"pg_dump "
    f"-h {PROD_HOST} -U {PROD_USER} -d {PROD_DB} "
    f"--schema=public --schema=etl "
    f"--no-owner --no-acl -Fc "
    f"> {REMOTE_DUMP} 2>/tmp/hml_dump_err.log && echo '__DUMP_OK__'"
)
stdin, stdout, stderr = ssh.exec_command(cmd, timeout=600)
output = stdout.read().decode()
err = stderr.read().decode()

if "__DUMP_OK__" not in output:
    # Pega o log de erro do dump
    _, err_out, _ = ssh.exec_command(f"cat /tmp/hml_dump_err.log 2>/dev/null || echo 'sem log'")
    dump_err = err_out.read().decode()
    print(f"ERRO no pg_dump:\nstdout: {output}\nstderr: {err}\ndump log: {dump_err}")
    ssh.close()
    sys.exit(1)

# Tamanho do arquivo remoto
_, size_out, _ = ssh.exec_command(f"du -sh {REMOTE_DUMP}")
size = size_out.read().decode().strip()
print(f"      OK Dump criado no VPS ({size})")

print(f"[3/3] Baixando dump via SFTP para {LOCAL_DUMP}...")
sftp = ssh.open_sftp()
file_size = sftp.stat(REMOTE_DUMP).st_size

downloaded = [0]
def progress(transferred, total):
    pct = transferred * 100 // total
    mb = transferred / 1024 / 1024
    print(f"\r      {pct}% ({mb:.1f} MB)", end="", flush=True)

sftp.get(REMOTE_DUMP, LOCAL_DUMP, callback=progress)
sftp.close()
ssh.close()

print(f"\n      OK Salvo em {LOCAL_DUMP}")
print("\nDump concluído com sucesso!")
