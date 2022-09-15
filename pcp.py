#!python3
""" Set up our processes for PCP. See pcp_base.py """

from pcp_base import pcp,Controllable,AsgiProcess,PythonProcess,ProcessGroup,getargs
from utils import import_modules 
import DApps

# processes:
AsgiProcess(name='api',app='frontend.dapp_api:app',port=8001)
AsgiProcess(name='market',app='frontend.market_api:app',port=8002)
AsgiProcess(name='subscription',app='frontend.subscription_api:app',port=8003)


portbase = 9001
processes = []
for i,module in enumerate(import_modules.listAllSubPackages(DApps)):
    app = module+':app'
    name = module.split('.')[-1]

    if name != '__init__':
        processes.append(AsgiProcess(name='DApp.'+name,app=app,port=portbase+1))
dapps = ProcessGroup(name='DApps',processes=processes)



# groups, referring to processes
ddh_base = ProcessGroup(name='ddh_base',processes=Controllable.get('api','market'))
ddh = ProcessGroup(name='ddh',processes=ddh_base.processes+dapps.processes)



if __name__ == '__main__':
    pcp()