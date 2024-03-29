import asyncio
import httpx

async def get_single(url, client):
    """ Auxiliary method to get the resource behind an individual url

    Parameters
    ----------
    url : str
        URL to get
    client : httpx Async client

    Returns
    -------
    requests.Response object
        The response
    """
    resp = await client.request(method = 'GET', url = url)
    return resp

async def get_all(urls):
    """ Method to get resources behind several urls asynchronously (parallelized)

    Parameters
    ----------
    urls : list of str
        The URLS to get

    Returns
    -------
    list of dicts
        Objects behind urls
    """
    async with httpx.AsyncClient(limits = httpx.Limits(max_connections=1000, max_keepalive_connections = 200)) as client:
        tasks = [get_single(url, client) for url in urls]
        responses = await asyncio.gather(*tasks)
    if any([response.status_code not in [200, 410]  for response in responses]):
        import pdb
        pdb.set_trace()
    responses = [response.json() for response in responses]
    return responses
