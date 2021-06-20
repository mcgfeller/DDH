""" Brokerage aspects:
    Offers, Contracts.
"""

from __future__ import annotations
import typing
import pydantic
import enum
from utils.pydantic_utils import NoCopyBaseModel,pyright_check

from core import permissions,errors,dapp,privacyIcons



@pyright_check
class Price(NoCopyBaseModel):
    currency : str = 'CHF'

@enum.unique
class Periodicity(str,enum.Enum):
    """ Subscription periodicity """

    D = 'daily'
    M = 'monthly'
    Y = 'yearly'
    O = 'once off'


    def __repr__(self): return self.value

@pyright_check
class SubscriptionPrice(Price):

    periodicity : Periodicity = Periodicity.M
    price_per_period : float

@pyright_check
class TransactionPrice(Price):
    ...


FreePrice = SubscriptionPrice(price_per_period=0.0,periodicity= Periodicity.O) 





@pyright_check
class TermsAndConditions(NoCopyBaseModel):
    """ There should be just a number of easy-to-understand Standard Terms and Conditions
    """
    privacy_icons = privacyIcons.PrivacyIcons

    StandardTACs : typing.ClassVar[dict[str,TermsAndConditions]] = {}


@pyright_check
class Offer(NoCopyBaseModel):
    """ The offer made by a DApp on a key """


    required_consents : permissions.Consents
    tac : TermsAndConditions
    dapp: permissions.DAppId
    price : Price = FreePrice

@pyright_check
class Contract(NoCopyBaseModel):

    offer = Offer
    acceptor = permissions.Principal

    @property 
    def offerer(self) -> permissions.Principal:
        """ contract offerer """
        return self.offer.dapp

