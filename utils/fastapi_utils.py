import httpx
from frontend import sessions

async def submit1_asynch(session : sessions.Session, base_url, url, params : dict = {}):
    """ Submit one request using httpx, with a given base_url and url.
        Copy the Authorization header from the session. 
        Return the json result. 
    """
    headers = {'Authorization':'Bearer '+session.token_str}
    async with httpx.AsyncClient(base_url=base_url,headers=headers) as client:
        j = await client.get(url,params=params)
    j.raise_for_status()
    d = j.json()
    return d