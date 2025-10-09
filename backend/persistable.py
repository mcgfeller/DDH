

import typing
import enum
import secrets
import zlib
import pydantic


from utils.pydantic_utils import DDHbaseModel
from core import transactions, common_ids, principals, errors, users
from backend import system_services
from frontend import user_auth


@enum.unique
class DataFormat(str, enum.Enum):
    """ Operations """

    dict = 'd'
    blob = 'b'
    json = 'j'


from backend import keyvault, storage


class NonPersistable(DDHbaseModel):
    """ NonPersistable, has itself as proxy """

    async def ensure_loaded(self, transaction: transactions.Transaction) -> NonPersistable:
        """ Non-persistables don't need to be loaded """
        return self

    def get_proxy(self) -> NonPersistable:
        """ Non-persistables don't need a Proxy """
        return self


class SupportsLoading(DDHbaseModel):

    async def ensure_loaded(self, transaction: transactions.Transaction) -> Persistable:
        """ must be refined to ensure self if Persistable """
        assert isinstance(self, Persistable)
        return self


class Persistable(SupportsLoading):
    """ class that provides methods to get persistent state.
        Works with storage.
    """

    Registry: typing.ClassVar[dict[str, type]] = {}
    id: common_ids.PersistId = pydantic.Field(default_factory=secrets.token_urlsafe)
    format: DataFormat = DataFormat.dict
    owner: principals.Principal | None = None

    @classmethod
    def __init_subclass__(cls):
        Persistable.Registry[cls.__name__] = cls

    async def store(self, transaction: transactions.Transaction):
        d = self.to_compressed()
        storage.Storage.store(self.id, d, transaction)
        return

    @classmethod
    async def load(cls, id: common_ids.PersistId, owner: principals.Principal | None, transaction: transactions.Transaction) -> typing.Self:
        d = storage.Storage.load(id, transaction)
        o = cls.from_compressed(d)
        return o

    async def delete(self, transaction: transactions.Transaction):
        await self.__class__.load(self.id, self.owner, transaction)  # verify encryption by loading
        storage.Storage.delete(self.id, transaction)
        return

    def to_compressed(self) -> bytes:
        return zlib.compress(self.to_json().encode(), level=-1)

    @classmethod
    def from_compressed(cls, data: bytes):
        return cls.from_json(zlib.decompress(data).decode())

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, j: str) -> Persistable:
        o = cls.model_validate_json(j)
        return o

    def get_key(self):
        raise NotImplementedError()

    def get_proxy(self) -> PersistableProxy:
        """ get a loadable proxy for us; idempotent. Reverse .ensureLoaded() """
        return PersistableProxy(id=self.id, classname=self.__class__.__name__, owner_id=self.owner.id if self.owner else None)


class PersistableProxy(SupportsLoading):
    """ Proxy with minimal data to load Persistable """
    id: common_ids.PersistId
    classname: str
    owner_id: common_ids.PrincipalId | None = None

    async def ensure_loaded(self, transaction: transactions.Transaction) -> Persistable:
        """ return an instantiaded Persistable subclass; idempotent """
        cls = Persistable.Registry[self.classname]
        cls = typing.cast(type[Persistable], cls)
        if self.owner_id:  # get owning user
            owner = user_auth.UserInDB.load_user(self.owner_id)
        else:
            owner = None
        obj = await cls.load(self.id, owner, transaction)
        assert isinstance(obj, cls)
        return obj

    def get_proxy(self) -> PersistableProxy:
        """ this is already a proxy """
        return self


class PersistAction(transactions.Action):

    obj: Persistable

    async def commit(self, transaction):
        """ store has currently not async support """
        await self.obj.store(transaction)
        return


class SystemDataPersistAction(PersistAction):
    """ Persist System Data """
    ...


class UserDataPersistAction(PersistAction):
    """ Persist User Data, storage DApp is user-specific.
        If add_to_dir is True, ensure node is in directory.
    """
    add_to_dir: bool = True

    async def commit(self, transaction):
        """ store has currently not async support """
        await self.obj.store(transaction)
        if self.add_to_dir:
            self.obj.ensure_in_dir(self.obj.key, transaction)
        return
