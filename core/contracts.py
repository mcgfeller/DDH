""" Brokerage aspects:
    Offers, Contracts.
"""

from __future__ import annotations
import typing
import pydantic
import enum
import decimal
from utils.pydantic_utils import NoCopyBaseModel

from core import permissions,errors,privacyIcons,principals




class Price(NoCopyBaseModel):
    currency : str = 'CHF'
    amount = decimal.Decimal 



@enum.unique
class Periodicity(str,enum.Enum):
    """ Subscription periodicity """

    D = 'daily'
    M = 'monthly'
    Y = 'yearly'
    O = 'once off'


    def __repr__(self): return self.value


class SubscriptionPrice(Price):

    periodicity : Periodicity = Periodicity.M
    price_per_period : float


class TransactionPrice(Price):
    ...


FreePrice = SubscriptionPrice(price_per_period=0.0,periodicity= Periodicity.O) 



class CancellationTerms(NoCopyBaseModel):

    runs_until_cancelled : bool = True
    cancellation_days : int = 1
    auto_expires_in_days : typing.Optional[int] = None



class TermsAndConditions(NoCopyBaseModel):
    """ There should be just a number of easy-to-understand Standard Terms and Conditions
    """


    StandardTACs : typing.ClassVar[dict[str,TermsAndConditions]] = {}

    privacy_icons = privacyIcons.PrivacyIcons
    cancellation_terms = CancellationTerms



class Offer(NoCopyBaseModel):
    """ The offer made by a DApp on a key """


    required_consents : permissions.Consents
    tac : TermsAndConditions
    dapp: principals.DAppId
    price : Price = FreePrice


class Contract(NoCopyBaseModel):

    offer = Offer
    acceptor = principals.Principal

    @property 
    def offerer(self) -> principals.Principal:
        """ contract offerer """
        return self.offer.dapp

