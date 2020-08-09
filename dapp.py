""" Support for DApps """

import core
import typing

class DApp(core.NoCopyBaseModel):
    

    owner : core.Principal

class DAppStore(core.NoCopyBaseModel):

    apps : typing.Dict[str,DApp] = {}
    
