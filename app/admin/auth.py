"""
app/admin/auth.py — SQLAdmin authentication backend.

Uses a signed session cookie. Credentials are loaded from settings so they
can be rotated via environment variables without a redeploy.
"""

from sqladmin.authentication import AuthenticationBackend
from starlette.requests import Request
from starlette.responses import RedirectResponse

from app.config import settings


class AdminAuth(AuthenticationBackend):

    async def authenticate(self, request: Request) -> bool:
        token = request.session.get("admin_authenticated")
        return token is True

    async def login(self, request: Request) -> bool:
        form = await request.form()
        username = form.get("username", "")
        password = form.get("password", "")
        if username == settings.admin_username and password == settings.admin_password:
            request.session["admin_authenticated"] = True
            return True
        return False

    async def logout(self, request: Request) -> bool:
        request.session.clear()
        return True
