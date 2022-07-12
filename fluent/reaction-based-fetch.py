from aiohttp import web, web_app
from etl.processor import DictEntryProcessor
import asyncio
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.models import Subscribe, Unsubscribe, StreamParameter

class LedgerSubscriptionDataSource:

    async def astart(self):
        ledger_update_sub_req = Subscribe(streams=[StreamParameter.LEDGER])
        # NOTE: this code will run forever without a timeout, until the process is killed
        async with AsyncWebsocketClient("wss://s1.ripple.com") as client:
            # one time subscription
            await client.send(ledger_update_sub_req)

            async for message in client:
                print(message)

            #await self.entry_processor.process()
"""
TODO:
Upon every ledger update:
+ get the transactions included in the ledger
+ given ledger, 
  + retrieve the transaction information
  + retrieve all the book offers

# Reference
- https://docs.aiohttp.org/en/stable/web_advanced.html#background-tasks
"""

async def handle(request):
    name = request.match_info.get('name', "Anonymous")
    text = "Hello, " + name
    return web.Response(text=text)


async def amain():
    pass


async def start_background_tasks(
    app: web_app.Application,
):
    ledger_data_source = LedgerSubscriptionDataSource()
    app["ledger_create_listener"] = asyncio.create_task(ledger_data_source.astart())
    

async def cleanup_background_tasks(
    app: web_app.Application,
):
    app["ledger_create_listener"].cancel()
    await app["ledger_create_listener"]


if __name__ == "__main__":
    app = web.Application()
    app.add_routes([
        web.get('/', handle),
        web.get('/{name}', handle),
    ])
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    web.run_app(app)