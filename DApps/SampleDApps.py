""" Set up a few fake Data Apps """
from __future__ import annotations

import datetime
import typing

import fastapi
import fastapi.security
import pydantic
from core import (common_ids, dapp_attrs, keys, nodes, permissions, principals, users,
                  relationships, schemas, transactions)
from schema_formats import py_schema
from frontend import fastapi_dapp
app = fastapi.FastAPI()
app.include_router(fastapi_dapp.router)


class SampleDApps(dapp_attrs.DApp):

    version = '0.2'

    owner: typing.ClassVar[principals.Principal]
    schemakey: typing.ClassVar[keys.DDHkeyVersioned]
    transforms_into: typing.ClassVar[keys.DDHkeyVersioned | None] = None
    provides_schema: bool = pydantic.Field(
        False, description="True if schemakey is not only defined by this DApp, but also provided.")

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.references = relationships.Reference.defines(self.schemakey)
        if self.provides_schema:
            self.references += relationships.Reference.provides(self.schemakey)
        if self.transforms_into:
            self.references.extend(relationships.Reference.provides(self.transforms_into))

    def get_schemas(self) -> dict[keys.DDHkeyVersioned, schemas.AbstractSchema]:
        """ Obtain initial schema for DApp """
        return {self.schemakey: py_schema.PySchema(schema_element=py_schema.PySchemaElement.create_from_elements('DummySchema'))}

    def execute(self, op: nodes.Ops, access: permissions.Access, transaction: transactions.Transaction, key_split: int, data: dict | None = None, q: str | None = None):
        """ obtain data by recursing to schema """
        if op == nodes.Ops.get:
            here, selection = access.ddhkey.split_at(key_split)
            d = {}
        else:
            raise ValueError(f'Unsupported {op=}')
        return d

    @classmethod
    def bootstrap(cls, session, pillars: dict) -> tuple[dapp_proxy.DAppProxy]:
        """ Create a series of DApps with network only """
        apps = (

            cls(
                id='SwisscomRoot',
                description="Owner of the Swisscom schema",
                owner=users.User(id='swisscom', name='Swisscom (fake account)'),
                schemakey=keys.DDHkeyVersioned0(key="//org/swisscom.com"),
                catalog=common_ids.CatalogCategory.living,
            ),

            RestrictedUserDApp(
                id='SwisscomEmpDApp',
                description="Swisscom Employee Data App",
                owner=users.User(id='swisscom', name='Swisscom (fake account)'),
                schemakey=keys.DDHkeyVersioned0(key="//org/swisscom.com/employees"),
                provides_schema=True,
                # 1 higher than needed by TaxCalc
                transforms_into=keys.DDHkeyVersioned0(key="//p/employment/salary"),
                catalog=common_ids.CatalogCategory.employment,
            ),

            cls(
                id='SBBroot',
                description="Owner of the SBB schema",
                owner=users.User(id='sbb', name='SBB (fake account)'),
                schemakey=keys.DDHkeyVersioned0(key="//org/sbb.ch"),
                catalog=common_ids.CatalogCategory.living,
            ),

            RestrictedUserDApp(
                id='SBBempDApp',
                description="SBB Staff Data App",
                owner=users.User(id='sbb', name='SBB (fake account)'),
                schemakey=keys.DDHkeyVersioned0(key="//org/sbb.ch/staff"),
                provides_schema=True,
                transforms_into=keys.DDHkeyVersioned0(key="//p/employment/salary/statements"),
                catalog=common_ids.CatalogCategory.employment,
            ),

            cls(
                id='TaxCalc',
                description="A Tax calculator, defines the Tax Declaration AbstractSchema",
                owner=users.User(id='privatetax', name='Private Tax (fake account)'),
                schemakey=keys.DDHkeyVersioned0(key="//p/finance/tax/declaration"),
                provides_schema=True,
                catalog=common_ids.CatalogCategory.finance,
            ).add_reference(relationships.Reference.requires(
                keys.DDHkeyRange(key="//p/employment/salary/statements:::>=0"),
                keys.DDHkeyRange(key="//p/finance/holdings/portfolio:::>=0")
            )),

            cls(
                id='CSroot',
                description="Owner of the Credit Suisse schema",
                owner=users.User(id='cs', name='Credit Suisse (fake account)'),
                schemakey=keys.DDHkeyVersioned0(key="//org/credit-suisse.com"),
                catalog=common_ids.CatalogCategory.finance,
            ),

            cls(
                id='CSportfolio',
                description="Portfolio API",
                owner=users.User(id='cs', name='Credit Suisse (fake account)'),
                schemakey=keys.DDHkeyVersioned0(key="//org/credit-suisse.com/clients/portfolio/account"),
                provides_schema=True,
                catalog=common_ids.CatalogCategory.finance,
            ),


            cls(
                id='UBSroot',
                description="Owner of the UBS schema",
                owner=users.User(id='ubs', name='UBS (fake account)'),
                schemakey=keys.DDHkeyVersioned0(key="//org/ubs.com"),
                catalog=common_ids.CatalogCategory.finance,
            ),

            cls(
                id='UBSaccount',
                description="Portfolio API",
                owner=users.User(id='ubs', name='UBS (fake account)'),
                schemakey=keys.DDHkeyVersioned0(key="//org/ubs.com/switzerland/customer/account"),
                provides_schema=True,
                transforms_into=keys.DDHkeyVersioned0(key="//p/finance/holdings/portfolio"),
                catalog=common_ids.CatalogCategory.finance,
                estimatedCosts=dapp_attrs.EstimatedCosts.medium,
            ),

            cls(
                id='AccountAggregator',
                description="Bank account aggregator, defines holdings",
                owner=users.User(id='coolfinance', name='Cool Finance Startup (fake account)'),
                schemakey=keys.DDHkeyVersioned0(key="//p/finance/holdings"),
                provides_schema=True,
                estimatedCosts=dapp_attrs.EstimatedCosts.medium,
                catalog=common_ids.CatalogCategory.finance,
            ).add_reference(relationships.Reference.requires(
                keys.DDHkeyRange(key="//org/credit-suisse.com/clients/portfolio/account:::>=0"),
                keys.DDHkeyRange(key="//org/ubs.com/switzerland/customer/account:::>=0"),
            )),


        )
        return apps


class RestrictedUserDApp(SampleDApps):

    def availability_user_dependent(self) -> bool:
        """ is the availability dependent on the user, e.g., for employee DApps.
            the concrete availability can be determined by .availability_for_user()
        """
        return True


def get_apps() -> tuple[dapp_attrs.DApp]:
    apps = SampleDApps.bootstrap(None, {})
    return apps


fastapi_dapp.get_apps = get_apps

if __name__ == "__main__":  # Debugging
    import uvicorn
    import os
    port = 9001
    os.environ['port'] = str(port)
    uvicorn.run(app, host="0.0.0.0", port=port)
