import asyncio
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List
import logging
from xrpl.asyncio.clients.async_websocket_client import AsyncWebsocketClient
from xrpl.models.requests.ledger import Ledger


logger = logging.getLogger(__name__)


class FetchProcessor:
    def __init__(serf,
    ):
        pass

    def process(self,
        entry: Dict[str, Any]
    ):
        pass


class ExtractTokenPairsProcessor:
    def process(self,
        entry: Dict[str, Any]
    ):
        # extract all the token pairs
        transactions = entry.get(
            "result", {}).get(
            "ledger", {}).get(
            "transactions", [])
        for txn in transactions:
            print(txn)


class LedgerIndexAsyncIterator:
    def __init__(self,
        aqueue: asyncio.Queue,
    ):
        self.aq = aqueue

    def __aiter__(self):
        return self    

    async def __anext__(self):
        idx = await self.aq.get()
        await self.aq.put(idx - 1)
        await asyncio.sleep(1)
        return idx 
        #raise StopIteration


class XRPLAsyncFetcher:
    def __init__(self,
        url: str,
        processors: List[FetchProcessor],
    ):
        self.url = url
        self.processors = processors

    async def _consume(self,
        aitr: AsyncIterator,
    ):
        async for entry in aitr:
            for proc in self.processors:
                proc.process(entry)

    async def astart(self,
        request_execution: Callable[[AsyncWebsocketClient], Awaitable[None]],
    ):
        logger.info("[XRPLAsyncFetcher] Start fetching.")
        consumption_task = None
        async with AsyncWebsocketClient(self.url) as client:

            # start the consumer
            consumption_task = asyncio.create_task(self._consume(
                aitr = client,
            ))

            await request_execution(
                async_websocket_client = client,
            )
        
        if consumption_task:
            consumption_task.cancel()


class LedgerIterationExecutioner:
    def __init__(self,
        ledger_index_async_iterator: AsyncIterator,
    ):
        self.ledger_index_async_iterator = ledger_index_async_iterator

    async def iterate_ledger_request(self,
        async_websocket_client: AsyncWebsocketClient,
    ):
        async for current_ledger_index in self.ledger_index_async_iterator:
            print("[LedgerIterationExecutioner] " + 
                f"Current ledger index [{current_ledger_index}].")
            req = Ledger(
                ledger_index = current_ledger_index,
                transactions = True,
                expand = True,
            )
            await async_websocket_client.send(req)


BookOfferRequest = collection.namedtuple(
    "BookOfferRequest", 
    ["ledger_index", "tokenA", "tokenB"],
)

class BookOfferRequestExecutioner:
    def __init__(self,
        bookoffer_req_queue: asyncio.Queue,
    ):
        self.bookoffer_req_queue = bookoffer_req_queue

    async def iterate_ledger_request(self,
        async_websocket_client: AsyncWebsocketClient,
    ):
        while True:
            # bookoffer_req_queue
            req = await bookoffer_req_queue.get()
            if type(req) != BookOfferRequest:
                # TODO: warnings

            # TODO: Make book offer request


async def amain():
    url = "wss://s1.ripple.com"

    ledger_index_queue = asyncio.Queue()
    ledger_index_queue.put_nowait(72631379)
    
    ledger_itr = LedgerIndexAsyncIterator(aqueue = ledger_index_queue)
    ledger_request_executioner = LedgerIterationExecutioner(ledger_itr)

    async_ledger_fetcher = XRPLAsyncFetcher(
        url = url,
        processors = [],
    )
    await async_ledger_fetcher.astart(
        request_execution = ledger_request_executioner.iterate_ledger_request,
    )

    bookoffer_req_queue = asyncio.Queue()
    bookoffer_request_executioner = BookOfferRequestExecutioner(
        bookoffer_req_queue = bookoffer_req_queue,
    )
    async_bookoffer_fetcher = XRPLAsyncFetcher(
        url = url,
        processors = [],
    )
    await async_bookoffer_fetcher.astart(
        request_execution = bookoffer_request_executioner.iterate_request,
    )


if __name__ == "__main__":
    asyncio.run(amain())
