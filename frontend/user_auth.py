""" Provisional User and Token management
    See https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/
"""
from __future__ import annotations
import fastapi
import typing
import pydantic
import datetime

import fastapi.security
import jose
import jose.jwt
import passlib.context
from utils.pydantic_utils import DDHbaseModel, utcnow

from core import errors, principals, users, keys
from frontend import sessions


oauth2_scheme = fastapi.security.OAuth2PasswordBearer(tokenUrl="token")
pwd_context = passlib.context.CryptContext(schemes=["bcrypt"], deprecated="auto")

# to get a string like this run:
# openssl rand -hex 32
SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


def get_password_hash(password):
    return pwd_context.hash(password)


FAKE_USERS_DB = {
    "mgf": {
        "id": "mgf",
        "name": "Martin Gfeller",
        "email": "martin.gfeller@swisscom.com",
        "hashed_password": get_password_hash("secret"),
    },
    "admin": {
        "id": "admin",
        "name": "DDH admin",
        "email": "martin.gfeller@swisscom.com",
        "hashed_password": get_password_hash("secret"),

    },

    "another": {
        "id": "another",
        "name": "just another user",
        "email": "nobody@swisscom.com",
        "hashed_password": get_password_hash("secret"),
    },

    "another3": {
        "id": "another3",
        "name": "just yet another user",
        "email": "nobody@swisscom.com",
        "hashed_password": get_password_hash("secret"),
    },

    "lise": {
        "id": "lise",
        "name": "User for consent testing",
        "email": "nobody@swisscom.com",
        "hashed_password": get_password_hash("secret"),
    },

    "jeffrey": {
        "id": "jeffrey",
        "name": "User for consent testing",
        "email": "nobody@swisscom.com",
        "hashed_password": get_password_hash("secret"),
    },

    "laura": {
        "id": "laura",
        "name": "User for consent testing",
        "email": "nobody@swisscom.com",
        "hashed_password": get_password_hash("secret"),
    },
}


class Token(DDHbaseModel):
    access_token: str
    token_type: str


class TokenData(DDHbaseModel):
    id: str


class UserInDB(users.User):
    hashed_password: str  # type: ignore

    @ classmethod
    def load(cls, id) -> UserInDB:
        """ Load user from DB, or raise NotFound """
        u = FAKE_USERS_DB.get(id, None)
        if u:
            return cls(**u)
        else:
            raise errors.NotFound(f'User not found {id}')

    def as_user(self) -> users.User:
        """ return user only """
        return users.User(**self.model_dump(include=users.User.model_fields.keys()))

    @ classmethod
    def load_user(cls, id) -> principals.Principal | UserInDB:
        """ return Principal or UserInDB """
        return principals.CommonPrincipals.get(id) or cls.load(id)


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_user(db, userid: str):
    if userid in db:
        user_dict = db[userid]
        return UserInDB(**user_dict)
    else: return None  # be explicit


def get_dappid(dappid: str) -> principals.DAppId:
    """ Verify the id of the DApp. This is largely provisional.
    """
    from core import pillars
    if dappid not in pillars.DAppManager.DAppsById:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_404_NOT_FOUND,
            detail=f"Invalid DApp id {dappid}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return typing.cast(principals.DAppId, dappid)


def authenticate_user(fake_db, userid: str, password: str):
    user = get_user(fake_db, userid)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


def create_access_token(data: dict, expires_delta: datetime.timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = utcnow() + expires_delta
    else:
        expire = utcnow() + datetime.timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jose.jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_session(token: str = fastapi.Depends(oauth2_scheme)):
    credentials_exception = fastapi.HTTPException(
        status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jose.jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        userid: str = typing.cast(str, payload.get("sub"))
        if userid is None:
            raise credentials_exception
        token_data = TokenData(id=userid)
    except jose.JWTError:
        raise credentials_exception
    user = get_user(FAKE_USERS_DB, userid=token_data.id)
    if user is None:
        raise credentials_exception
    dappid = payload.get('x_dapp')  # verified dapp id
    return sessions.Session(user=user, dappid=dappid, token_str=token)


async def get_current_active_user(current_session: sessions.Session = fastapi.Depends(get_current_session)):
    return current_session.user


async def login_for_access_token(form_data: fastapi.security.OAuth2PasswordRequestForm):
    """ get user from login form, including optional dappid,
        which provisionally comes from client_id.
    """
    user = authenticate_user(FAKE_USERS_DB, form_data.username, form_data.password)
    if not user:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect user or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    dappid = get_dappid(form_data.client_id) if form_data.client_id else None
    access_token_expires = datetime.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.id, 'xdapp': dappid, }, expires_delta=access_token_expires
    )
    return user, dappid, {"access_token": access_token, "token_type": "bearer"}


def get_principals(selection: str) -> list[principals.Principal]:
    """ check string containing one or more Principals, separated by comma,
        return them as Principal.
        First checks CommonPrincipals defined here, then user_auth.UserInDB.
    """

    ids = selection.split(keys.DDHkey.OwnerDelimiter)
    princs = []
    for i in ids:
        p = principals.CommonPrincipals.get(i)
        if not p:
            p = UserInDB.load(id=i)
            assert p  # load must raise error if not found
        princs.append(p)
    return princs
