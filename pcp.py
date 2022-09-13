#!python3
""" Set up our processes for PCP. See pcp_base.py """

from pcp_base import pcp,Controllable,AsgiProcess,PythonProcess,ProcessGroup
from utils import import_modules 
import DApps

# processes:
AsgiProcess(name='api',app='frontend.dapp_api:app',port=8001)
AsgiProcess(name='market',app='frontend.market_api:app',port=8002)


for module in import_modules.listAllSubPackages(DApps):
    print(module)


# groups, referring to processes
ddh = ProcessGroup(name='ddh',processes=Controllable.get('api','market'))

if __name__ == '__main__':
    pcp()