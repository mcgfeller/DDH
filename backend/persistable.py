
from __future__ import annotations
import typing
import enum
import secrets
import zlib
import pydantic


from utils.pydantic_utils import NoCopyBaseModel
from core import transactions,common_ids


@enum.unique
class DataFormat(str,enum.Enum):
    """ Operations """

    dict = 'd'
    blob = 'b'
    json = 'j'    



from backend import keyvault,storage

class NonPersistable(NoCopyBaseModel):
    """ NonPersistable, has itself as proxy """


    def ensure_loaded(self, transaction : transactions.Transaction) -> NonPersistable:
        """ Non-persistables don't need to be loaded """
        return self

    def get_proxy(self) -> NonPersistable:
        """ Non-persistables don't need a Proxy """
        return self

class Persistable(NoCopyBaseModel):
    """ class that provides methods to get persistent state.
        Works with storage.
    """

    Registry : typing.ClassVar[dict[str,type]] = {}
    id : common_ids.PersistId = pydantic.Field(default_factory=secrets.token_urlsafe)
    format : DataFormat = DataFormat.dict

    @classmethod
    def __init_subclass__(cls):
        Persistable.Registry[cls.__name__] = cls

    def store(self, transaction: transactions.Transaction ):
        d = self.to_compressed()
        storage.Storage.store(self.id,d, transaction)
        return

    @classmethod
    def load(cls, id:common_ids.PersistId,  transaction: transactions.Transaction ):
        d = storage.Storage.load(id, transaction)
        o = cls.from_compressed(d)
        return o

    def delete(self, transaction: transactions.Transaction ):
        self.__class__.load(self.id, transaction) # verify encryption by loading
        storage.Storage.delete(self.id, transaction)
        return

    def to_compressed(self) -> bytes:
        return zlib.compress(self.to_json().encode(), level=-1)

    @classmethod
    def from_compressed(cls,data : bytes):
        return cls.from_json(zlib.decompress(data).decode())

    def to_json(self) -> str:
        return self.json()

    @classmethod
    def from_json(cls, j :str) -> Persistable:
        o = cls.parse_raw(j)
        return o

    def get_key(self):
        raise NotImplementedError()

    def ensure_loaded(self, transaction : transactions.Transaction) -> Persistable:
        """ self is already loaded, make operation idempotent """
        return self

    def get_proxy(self) -> PersistableProxy:
        """ get a loadable proxy for us; idempotent. Reverse .ensureLoaded() """
        return PersistableProxy(id=self.id,classname=self.__class__.__name__)

class PersistableProxy(NoCopyBaseModel):
    """ Proxy with minimal data to load Persistable """
    id : common_ids.PersistId
    classname: str

    def ensure_loaded(self, transaction : transactions.Transaction) -> Persistable:
        """ return an instantiaded Persistable subclass; idempotent """
        cls = Persistable.Registry[self.classname]
#        cls = typing.cast(Persistable,cls)
        obj = cls.load(self.id,transaction)
        assert isinstance(obj,cls)
        return obj

    def get_proxy(self) -> PersistableProxy:
        """ this is already a proxy """
        return self
