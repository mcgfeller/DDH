""" Set up a few fake Data Apps """
from __future__ import annotations
import typing

import pydantic

from core import keys,permissions,schemas,nodes,principals,transactions,relationships,common_ids
from core import dapp


class SampleDApps(dapp.DApp):

    owner : typing.ClassVar[principals.Principal] 
    schemakey : typing.ClassVar[keys.DDHkey] 
    transforms_into : typing.ClassVar[typing.Optional[keys.DDHkey]]= None
    provides_schema : bool = pydantic.Field(False,description="True if schemakey is not only defined by this DApp, but also provided.")

    def __init__(self,*a,**kw):
        super().__init__(*a,**kw)
        self.references = relationships.Reference.defines(self.schemakey) 
        if self.provides_schema:
            self.references += relationships.Reference.provides(self.schemakey) 
        if self.transforms_into:
            self.references.extend(relationships.Reference.provides(self.transforms_into))
 
    def get_schemas(self) -> dict[keys.DDHkey,schemas.Schema]:
        """ Obtain initial schema for DApp """
        return {self.schemakey:schemas.PySchema(schema_element=pydantic.create_model('DummySchema',__base__=schemas.SchemaElement))}


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
                provides_schema = True,
                transforms_into = keys.DDHkey(key="//p/employment/salary"), # 1 higher than needed by TaxCalc
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
                provides_schema = True,
                transforms_into = keys.DDHkey(key="//p/employment/salary/statements"),
                catalog = common_ids.CatalogCategory.employment,
                ),

            cls(
                id='TaxCalc',
                description = "A Tax calculator, defines the Tax Declaration Schema",
                owner=principals.User(id='privatetax',name='Private Tax (fake account)'),
                schemakey = keys.DDHkey(key="//p/finance/tax/declaration"), 
                provides_schema = True,
                catalog = common_ids.CatalogCategory.finance,
                ).add_reference(relationships.Reference.requires(
                    keys.DDHkey(key="//p/employment/salary/statements"),
                    keys.DDHkey(key="//p/finance/holdings/portfolio")
                    )),

            cls(
                id='CSroot', 
                description = "Owner of the Credit Suisse schema",
                owner=principals.User(id='cs',name='Credit Suisse (fake account)'),
                schemakey = keys.DDHkey(key="//org/credit-suisse.com"), 
                catalog = common_ids.CatalogCategory.finance,
                ),

            cls(
                id='CSportfolio', 
                description = "Portfolio API",
                owner=principals.User(id='cs',name='Credit Suisse (fake account)'),
                schemakey = keys.DDHkey(key="//org/credit-suisse.com/clients/portfolio/account"), 
                provides_schema = True,
                catalog = common_ids.CatalogCategory.finance,
                ),


            cls(
                id='UBSroot', 
                description = "Owner of the UBS schema",
                owner=principals.User(id='ubs',name='UBS (fake account)'),
                schemakey = keys.DDHkey(key="//org/ubs.com"), 
                catalog = common_ids.CatalogCategory.finance,
                ),

            cls(
                id='UBSaccount', 
                description = "Portfolio API",
                owner=principals.User(id='ubs',name='UBS (fake account)'),
                schemakey = keys.DDHkey(key="//org/ubs.com/switzerland/customer/account"), 
                provides_schema = True,
                transforms_into = keys.DDHkey(key="//p/finance/holdings/portfolio"),
                catalog = common_ids.CatalogCategory.finance,
                ),

            cls(
                id='AccountAggregator',
                description = "Bank account aggregator, defines holdings",
                owner=principals.User(id='coolfinance',name='Cool Finance Startup (fake account)'),
                schemakey = keys.DDHkey(key="//p/finance/holdings"), 
                provides_schema = True,
                estimatedCosts = dapp.EstimatedCosts.medium,
                catalog = common_ids.CatalogCategory.finance,
                ).add_reference(relationships.Reference.requires(
                    keys.DDHkey(key="//org/credit-suisse.com/clients/portfolio/account"),
                    keys.DDHkey(key="//org/ubs.com/switzerland/customer/account"),
                    )),


        )
        return  apps

class RestrictedUserDApp(SampleDApps):

    def availability_user_dependent(self) -> bool:
        """ is the availability dependent on the user, e.g., for employee DApps.
            the concrete availability can be determined by .availability_for_user()
        """
        return True 

