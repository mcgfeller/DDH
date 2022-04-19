""" Set up a few fake Data Apps """
from __future__ import annotations
import typing

from core import keys,permissions,schemas,nodes,principals,transactions,relationships,common_ids
from core import dapp


class SampleDApps(dapp.DApp):

    owner : typing.ClassVar[principals.Principal] 
    schemakey : typing.ClassVar[keys.DDHkey] 
    transforms_into : typing.ClassVar[typing.Optional[keys.DDHkey]]= None

    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self.references = relationships.Reference.provides(self.schemakey) 
        if self.transforms_into:
            self.references.extend(relationships.Reference.provides(self.transforms_into))
 
    def get_schemas(self) -> dict[keys.DDHkey,schemas.Schema]:
        """ Obtain initial schema for DApp """
        return {self.schemakey:schemas.PySchema(schema_element=DummySchema)}


    def execute(self, op: nodes.Ops, access : permissions.Access, transaction: transactions.Transaction, key_split : int, data : typing.Optional[dict] = None, q : typing.Optional[str] = None):
        """ obtain data by recursing to schema """
        if op == nodes.Ops.get:
            here,selection = access.ddhkey.split_at(key_split)
            d = {}
        else:
            raise ValueError(f'Unsupported {op=}')
        return d






    @classmethod
    def bootstrap(cls,session,pillars : dict) -> tuple[dapp.DApp]:
        """ Create a series of DApps with network only """
        apps = (

            cls(
                id='SwisscomRoot',
                description = "Owner of the Swisscom schema",
                owner=principals.User(id='swisscom',name='Swisscom (fake account)'),
                schemakey = keys.DDHkey(key="//org/swisscom.com"), 
                catalog = common_ids.CatalogCategory.living,
                ),

            RestrictedUserDApp(
                id='SwisscomEmpDApp',
                description = "Swisscom Employee Data App",
                owner=principals.User(id='swisscom',name='Swisscom (fake account)'),
                schemakey = keys.DDHkey(key="//org/swisscom.com/employees"), 
                transforms_into = keys.DDHkey(key="//p/employment/salary/statements"),
                catalog = common_ids.CatalogCategory.employment,
                ),

            cls(
                id='SBBroot', 
                description = "Owner of the SBB schema",
                owner=principals.User(id='sbb',name='SBB (fake account)'),
                schemakey = keys.DDHkey(key="//org/sbb.ch"), 
                catalog = common_ids.CatalogCategory.living,
                ),

            RestrictedUserDApp(
                id='SBBempDApp',
                description = "SBB Staff Data App",
                owner=principals.User(id='sbb',name='SBB (fake account)'),
                schemakey = keys.DDHkey(key="//org/sbb.ch/staff"), 
                transforms_into = keys.DDHkey(key="//p/employment/salary/statements"),
                catalog = common_ids.CatalogCategory.employment,
                ),

            cls(
                id='TaxCalc',
                description = "A Tax calculator, defines the Tax Declaration Schema",
                owner=principals.User(id='privatetax',name='Private Tax (fake account)'),
                schemakey = keys.DDHkey(key="//p/finance/tax/declaration"), 
                catalog = common_ids.CatalogCategory.finance,
                ).add_reference(relationships.Reference.requires(keys.DDHkey(key="//p/employment/salary/statements"))),
        )
        return  apps

class RestrictedUserDApp(SampleDApps):

    def availability_user_dependent(self) -> bool:
        """ is the availability dependent on the user, e.g., for employee DApps.
            the concrete availability can be determined by .availability_for_user()
        """
        return True 

class DummySchema(schemas.SchemaElement):
    """ Must have some variable to have annotations """
    name : str = 'Dummy'

