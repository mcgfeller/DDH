""" Principals and Users """

from __future__ import annotations
import pydantic 
import datetime
import typing
import enum
import abc

from pydantic.errors import PydanticErrorMixin
from utils.pydantic_utils import NoCopyBaseModel
from . import errors,keys,common_ids





class Principal(NoCopyBaseModel):
    """ Abstract identification of a party """
    class Config:
        extra = pydantic.Extra.ignore # for parsing of subclass

    id : common_ids.PrincipalId
    Delim : typing.ClassVar[str] = ','

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

 # The identification of a DApp. We use a str for now, instead of subclass of Principal (which cannot be used in place of a str):
DAppId = typing.NewType('DAppId',str)

# Collect all common principals
CommonPrincipals = {p.id : p for p in (AllPrincipal,RootPrincipal,SystemUser)}
CommonPrincipals[keys.DDHkey.AnyKey] = RootPrincipal

