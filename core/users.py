""" Users """

from __future__ import annotations
import pydantic 
import datetime

from utils.pydantic_utils import NoCopyBaseModel
from . import errors,keys,common_ids,principals




class Profile(NoCopyBaseModel):
    """ Profile associated with a user """
    system_dapps : dict[str,str]

DefaultProfile = Profile(system_dapps={})

class User(principals.Principal):
    """ Concrete user, may login """
       
    name : str 
    email : pydantic.EmailStr|None = None
    created_at : datetime.datetime = pydantic.Field(default_factory=datetime.datetime.utcnow) # defaults to now
    profile : Profile = DefaultProfile



SystemUser = User(id='root',name='root')
principals.CommonPrincipals[SystemUser.id] = SystemUser



