""" Users """

from __future__ import annotations
import pydantic
import datetime
import enum

from utils.pydantic_utils import NoCopyBaseModel
from . import errors, keys, common_ids, principals
from backend import system_services




class Profile(NoCopyBaseModel):
    """ Profile associated with a user """
    system_services: system_services.ProfiledServices = system_services.ProfiledServices()


DefaultProfile = Profile()


class User(principals.Principal):
    """ Concrete user, may login """

    name: str
    email: pydantic.EmailStr | None = None
    created_at: datetime.datetime = pydantic.Field(
        default_factory=datetime.datetime.utcnow)  # defaults to now
    profile: Profile = DefaultProfile


SystemUser = User(id='root', name='root')
principals.CommonPrincipals[SystemUser.id] = SystemUser
