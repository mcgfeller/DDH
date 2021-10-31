""" Principals and Users """

from __future__ import annotations
import pydantic 
import datetime
import typing
import enum
import abc

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel
from . import errors,keys


PrincipalId = typing.NewType('PrincipalId', str)

class Principal(NoCopyBaseModel):
    """ Abstract identification of a party """
    class Config:
        extra = pydantic.Extra.ignore # for parsing of subclass

    id : PrincipalId
    Delim : typing.ClassVar[str] = ','

    @classmethod
    def get_principals(cls, selection: str) -> list[Principal]:
        """ check string containing one or more Principals, separated by comma,
            return them as Principal.
            First checks CommonPrincipals defined here, then user_auth.UserInDB.
        """
        from frontend import user_auth
        ids = selection.split(cls.Delim)
        principals = []
        for i in ids:
            p = CommonPrincipals.get(i)
            if not p:
                p = user_auth.UserInDB.load(id=i)
                assert p # load must raise error if not found
            principals.append(p)
        return principals

    def __eq__(self,other) -> bool:
        """ Principals are equal if their id is equal """
        return self.id == other.id if isinstance(other,Principal) else False

    def __hash__(self): 
        """ hashable on id """
        return hash(self.id)

    @classmethod
    def load(cls,id):
        raise errors.SubClass



AllPrincipal = Principal(id='_all_')
RootPrincipal = Principal(id='DDH')


class User(Principal):
    """ Concrete user, may login """
       
    name : str 
    email : typing.Optional[pydantic.EmailStr] = None
    created_at : datetime.datetime = pydantic.Field(default_factory=datetime.datetime.utcnow) # defaults to now

SystemUser = User(id='root',name='root')



class DAppId(Principal):
    """ The identification of a DApp. We use a Principal for now. """

    name : str

# Collect all common principals
CommonPrincipals = {p.id : p for p in (AllPrincipal,RootPrincipal,SystemUser)}
CommonPrincipals[keys.DDHkey.AnyKey] = RootPrincipal

