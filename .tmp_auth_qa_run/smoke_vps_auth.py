import paramiko


HOST = "5.189.163.33"
PORT = 22
USERNAME = "root"
PASSWORD = "cb5D75sc41Txr"


COMMANDS = [
    "curl -s http://127.0.0.1:8002/api/v1/health",
    "curl -s -o /tmp/auth_me.out -w '%{http_code}' http://127.0.0.1:8002/api/v1/auth/me",
    "cat /tmp/auth_me.out",
    "curl -s -o /tmp/auth_logout.out -w '%{http_code}' -H 'Content-Type: application/json' -d '{\"refresh_token\":\"x\"}' http://127.0.0.1:8002/api/v1/auth/logout",
    "cat /tmp/auth_logout.out",
]


def main() -> None:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        HOST,
        port=PORT,
        username=USERNAME,
        password=PASSWORD,
        timeout=20,
        banner_timeout=20,
        auth_timeout=20,
    )
    try:
        for command in COMMANDS:
            stdin, stdout, stderr = client.exec_command(command)
            print(f"CMD: {command}")
            print(stdout.read().decode("utf-8", errors="replace"))
            err = stderr.read().decode("utf-8", errors="replace")
            if err:
                print(f"ERR: {err}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
