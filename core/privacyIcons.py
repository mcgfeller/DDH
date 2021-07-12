from __future__ import annotations
import typing
import pydantic
import enum
from utils.pydantic_utils import NoCopyBaseModel

@enum.unique
class PrivacyIcon(str,enum.Enum):
    """ Privacy Icons, according to https://privacy-icons.ch/en/  """

    def __repr__(self): return self.value

class TypeOfPersonalData(PrivacyIcon):
    GeneralData = 'General Data'
    FinancialData = 'Financial Data'
    HealthData = 'Health Data'
    LocationData = 'Location Data'
    BiometricData = 'Biometric Data'
    IntimateData = 'Intimate Data'

class SourceOfPersonalData(PrivacyIcon):

    ProvidedData = 'Provided Data'
    CollectedData = 'Collected Data'
    ReceivedData = 'Received Data'

class PurposeOfProcessing(PrivacyIcon):
    Marketing = 'Marketing'
    ProductDevelopment = 'Product Development'
    OtherPurposes = 'Other Purposes'

class SpecialProcesses(PrivacyIcon):
    AutomatedDecisionMaking = 'Automated Decision-Making'
    Profiling = 'Profiling'

class PassingOnToThirdParties(PrivacyIcon):
    DataTransfers = 'Data Transfers'
    DataSale = 'Data Sale'

class PlaceOfProcessing(PrivacyIcon):
    Switzerland = 'Switzerland'
    Europe = 'Europe'
    Worldwide = 'Worldwide'


class PrivacyIcons(NoCopyBaseModel):

    included : set[PrivacyIcon] = pydantic.Field(default_factory=set,description='Positive icons')
    excluded : set[PrivacyIcon] = pydantic.Field(default_factory=set,description='Negative icons')
