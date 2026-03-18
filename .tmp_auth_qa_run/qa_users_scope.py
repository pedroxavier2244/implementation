import asyncio
import os
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from httpx import ASGITransport, AsyncClient

os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-for-testing-only-32chars!!")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost/testdb")
os.environ.setdefault("SMTP_USER", "test@test.com")
os.environ.setdefault("SMTP_PASSWORD", "testpass")
os.environ.setdefault("SMTP_FROM", "test@test.com")
os.environ.setdefault("DEBUG", "false")

from app.api.v1.schemas.users import UserRole
from app.core.security import create_access_token
from app.main import app
from app.db.session import get_db
from app.models.user import User


def build_user(email: str, full_name: str, role: UserRole, gestor_id: str | None = None):
    now = datetime.now(timezone.utc)
    user = User(
        id=str(uuid.uuid4()),
        email=email,
        full_name=full_name,
        role=role,
        gestor_id=gestor_id,
        hashed_password="hashed",
        is_active=True,
        created_at=now,
        updated_at=now,
    )
    return user


class FakeUserRepository:
    users: dict[str, User] = {}
    reset_tokens: list[dict] = []

    def __init__(self, db):
        self.db = db

    @classmethod
    def reset(cls):
        cls.users = {}
        cls.reset_tokens = []

    async def get_by_id(self, user_id: str):
        return self.users.get(user_id)

    async def get_by_email(self, email: str):
        for user in self.users.values():
            if user.email.lower() == email.lower():
                return user
        return None

    async def create(self, email: str, full_name: str, role: str, gestor_id: str | None = None):
        user = build_user(email, full_name, UserRole(role), gestor_id=gestor_id)
        user.hashed_password = None
        self.users[user.id] = user
        return user

    async def update(self, user_id: str, **fields):
        user = self.users[user_id]
        for key, value in fields.items():
            setattr(user, key, value)
        user.updated_at = datetime.now(timezone.utc)
        return user

    async def deactivate(self, user_id: str):
        user = self.users[user_id]
        user.is_active = False
        user.updated_at = datetime.now(timezone.utc)
        return user

    async def list_users(
        self,
        page: int = 1,
        page_size: int = 20,
        is_active=None,
        role=None,
        gestor_id=None,
    ):
        users = list(self.users.values())
        if is_active is not None:
            users = [u for u in users if u.is_active == is_active]
        if role is not None:
            users = [u for u in users if str(u.role) == role or getattr(u.role, "value", None) == role]
        if gestor_id is not None:
            users = [u for u in users if u.gestor_id == gestor_id]
        users.sort(key=lambda u: u.created_at, reverse=True)
        total = len(users)
        start = (page - 1) * page_size
        return users[start:start + page_size], total

    async def save_reset_token(self, user_id: str, token_hash: str, expires_at):
        self.reset_tokens.append(
            {"user_id": user_id, "token_hash": token_hash, "expires_at": expires_at}
        )
        return self.reset_tokens[-1]

    async def save_refresh_token(self, *args, **kwargs):
        return None

    async def get_refresh_token(self, *args, **kwargs):
        return None

    async def revoke_refresh_token(self, *args, **kwargs):
        return None

    async def revoke_all_user_tokens(self, *args, **kwargs):
        return None

    async def get_reset_token(self, *args, **kwargs):
        return None

    async def consume_reset_token(self, *args, **kwargs):
        return None

    async def set_password(self, *args, **kwargs):
        return None

    async def update_last_login(self, *args, **kwargs):
        return None


async def override_get_db():
    yield object()


async def run():
    FakeUserRepository.reset()
    admin = build_user("admin@test.com", "Admin", UserRole.admin)
    gestor = build_user("gestor@test.com", "Gestor", UserRole.gestor)
    operador = build_user(
        "operador@test.com",
        "Operador Time",
        UserRole.operador,
        gestor_id=gestor.id,
    )
    sem_gestor = build_user("semgestor@test.com", "Sem Gestor", UserRole.operador)
    outro_gestor = build_user("outrogestor@test.com", "Outro Gestor", UserRole.gestor)
    outro_operador = build_user(
        "outrooperador@test.com",
        "Outro Operador",
        UserRole.operador,
        gestor_id=outro_gestor.id,
    )
    for user in (admin, gestor, operador, sem_gestor, outro_gestor, outro_operador):
        FakeUserRepository.users[user.id] = user

    admin_token = create_access_token(admin.id, admin.email, UserRole.admin)
    gestor_token = create_access_token(gestor.id, gestor.email, UserRole.gestor)
    operador_token = create_access_token(operador.id, operador.email, UserRole.operador)

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        with patch("app.services.user_service.UserRepository", FakeUserRepository), patch(
            "app.integrations.email_client.send_invite_email",
            new=AsyncMock(return_value=None),
        ), patch(
            "app.services.audit_service.AuditService.log",
            new=AsyncMock(return_value=None),
        ):
            checks = []

            resp = await client.get(f"/api/v1/users/", headers={"Authorization": f"Bearer {admin_token}"})
            checks.append(("admin_list_all", resp.status_code, len(resp.json()["items"])))

            resp = await client.get(f"/api/v1/users/", headers={"Authorization": f"Bearer {gestor_token}"})
            gestor_items = resp.json()["items"]
            checks.append(("gestor_list_own_team", resp.status_code, [item["email"] for item in gestor_items]))

            resp = await client.post(
                "/api/v1/users/",
                json={"email": "novo-time@test.com", "full_name": "Novo Time", "role": "operador"},
                headers={"Authorization": f"Bearer {gestor_token}"},
            )
            checks.append(("gestor_create_operator", resp.status_code, resp.json().get("gestor_id")))

            resp = await client.post(
                "/api/v1/users/",
                json={"email": "novo-gestor@test.com", "full_name": "Novo Gestor", "role": "gestor"},
                headers={"Authorization": f"Bearer {gestor_token}"},
            )
            checks.append(("gestor_create_gestor_forbidden", resp.status_code, resp.json()))

            resp = await client.patch(
                f"/api/v1/users/{sem_gestor.id}",
                json={"gestor_id": gestor.id},
                headers={"Authorization": f"Bearer {gestor_token}"},
            )
            checks.append(("gestor_claim_unassigned", resp.status_code, resp.json().get("gestor_id")))

            resp = await client.patch(
                f"/api/v1/users/{operador.id}",
                json={"gestor_id": None},
                headers={"Authorization": f"Bearer {gestor_token}"},
            )
            checks.append(("gestor_remove_from_team", resp.status_code, resp.json().get("gestor_id")))

            resp = await client.patch(
                f"/api/v1/users/{outro_operador.id}",
                json={"full_name": "Hack"},
                headers={"Authorization": f"Bearer {gestor_token}"},
            )
            checks.append(("gestor_cannot_manage_other_team", resp.status_code, resp.json()))

            resp = await client.patch(
                f"/api/v1/users/{sem_gestor.id}",
                json={"role": "gestor"},
                headers={"Authorization": f"Bearer {gestor_token}"},
            )
            checks.append(("gestor_cannot_promote_operator", resp.status_code, resp.json()))

            resp = await client.delete(
                f"/api/v1/users/{outro_gestor.id}",
                headers={"Authorization": f"Bearer {gestor_token}"},
            )
            checks.append(("gestor_cannot_deactivate_other_gestor", resp.status_code, resp.json()))

            resp = await client.post(
                f"/api/v1/users/",
                json={"email": "x@test.com", "full_name": "X", "role": "operador"},
                headers={"Authorization": f"Bearer {operador_token}"},
            )
            checks.append(("operador_forbidden", resp.status_code, resp.json()))

            for item in checks:
                print(item)

    app.dependency_overrides.clear()


if __name__ == "__main__":
    run_sync = asyncio.run
    run_sync(run())
