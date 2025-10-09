""" Users """


import pydantic
import datetime
import enum

from utils.pydantic_utils import DDHbaseModel, utcnow
from . import errors, keys, common_ids, principals
from backend import system_services as m_system_services


class Profile(DDHbaseModel):
    """ Profile associated with a user """
    system_services: m_system_services.ProfiledServices = m_system_services.ProfiledServices()


DefaultProfile = Profile()


class User(principals.Principal):
    """ Concrete user, may login """

    name: str
    email: pydantic.EmailStr | None = None
    created_at: datetime.datetime = pydantic.Field(
        default_factory=utcnow)  # defaults to now
    profile: Profile = DefaultProfile


SystemUser = User(id='root', name='root')
principals.CommonPrincipals[SystemUser.id] = SystemUser
