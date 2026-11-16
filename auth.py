import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config import JWT_ALG, JWT_SECRET

bearer = HTTPBearer()


async def usuario_actual(cred: HTTPAuthorizationCredentials = Depends(bearer)) -> dict:
    try:
        return jwt.decode(cred.credentials, JWT_SECRET, algorithms=[JWT_ALG])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Token inválido")


def requiere_roles(*roles: str):
    async def checker(user: dict = Depends(usuario_actual)):
        if user["rol"] not in roles:
            raise HTTPException(403, "No tienes permisos para esta acción")
        return user
    return checker
