cd %~dp0
rem .venv\scripts\activate
rem "C:\Program Files\Python39\scripts\uvicorn" frontend.dapp_api:app --port 8001 --reload --no-use-colors
.venv\scripts\uvicorn DApps.MigrosDApp:app --port 8101 --reload --no-use-colors
