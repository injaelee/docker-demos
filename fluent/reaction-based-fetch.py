from aiohttp import web
from etl.processor import DictEntryProcessor

class LedgerSubscriptionDataSource:
    def __init__(self,
        processor: DictEntryProcessor,
    ):
        self.entry_processor = processor

    async def astart(self):
        ledger_update_sub_req = Subscribe(streams=[StreamParameter.LEDGER])
        # NOTE: this code will run forever without a timeout, until the process is killed
        with WebsocketClient(url) as client:
            # one time subscription
            client.send(ledger_update_sub_req)

            for message in client:
                print(message)

            await self.entry_processor.process()
"""



"""


async def handle(request):
    name = request.match_info.get('name', "Anonymous")
    text = "Hello, " + name
    return web.Response(text=text)

async def amain():
    pass

if __name__ == "__main__":
    app = web.Application()
    app.add_routes([
        web.get('/', handle),
        web.get('/{name}', handle),
    ])