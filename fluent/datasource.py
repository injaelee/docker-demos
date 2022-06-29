import asyncio
import collections
import cuid
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Union
import logging
from xrpl.asyncio.clients.async_websocket_client import AsyncWebsocketClient
from xrpl.models.requests.ledger import Ledger
from xrpl.models.currencies import XRP, IssuedCurrency
import sys
import traceback


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


class FetchAsyncProcessor:
    def __init__(serf,
    ):
        pass

    async def process(self,
        entry: Dict[str, Any]
    ):
        pass


class ExtractTokenPairsAsyncProcessor:
    def __init__(self,
        aqueue: asyncio.Queue,
    ):
        self.aq = aqueue

    async def process(self,
        entry: Dict[str, Any],
    ):
        try:
            await self._process(entry)
        except Exception as exp:
            traceback.print_exc()

    async def _process(self,
        entry: Dict[str, Any],
    ):
        logger.info("[ExtractTokenPairsAsyncProcessor] Start iteration.")

        # extract all the token pairs
        transactions = entry.get(
            "result", {}).get(
            "ledger", {}).get(
            "transactions", [])

        ledger_index = entry.get("result", {}).get("ledger_index")

        def build_currency(
            value: Union[str,Dict],
        ) -> Union[XRP, IssuedCurrency]:
            if type(value) == str:
                return XRP()

            amt = IssuedCurrency(
                currency = value.get("currency"),
                issuer = value.get("issuer"),
            )

            if amt.currency is None or amt.issuer is None:
                return None

            return amt

        pair_tuple_set = set()

        for txn in transactions:

            for node in txn.get("metaData", {}).get("AffectedNodes", []):

                data_node = node.get("ModifiedNode") or node.get("CreatedNode")
                if not data_node:
                    continue

                logger.info("Ledger entry type: %s", data_node.get("LedgerEntryType"))
                if data_node.get("LedgerEntryType") != "Offer":
                    # skip non offer induced changes
                    continue

                fields = data_node.get("FinalFields") or data_node.get("NewFields")
                taker_gets = fields.get("TakerGets")
                taker_pays = fields.get("TakerPays")

                if not taker_gets or not taker_pays:
                    # no need to continue without those data
                    continue

                taker_gets_currency = build_currency(taker_gets)
                taker_pays_currency = build_currency(taker_pays)

                pair_tuple_set.add((
                    taker_gets_currency,
                    taker_pays_currency
                ))

        for taker_gets_currency, taker_pays_currency in pair_tuple_set:
                logger.info(
                    "[ExtractTokenPairsAsyncProcessor] Enqueue book offer request: [%s], [%s]",
                    taker_gets_currency,
                    taker_pays_currency,
                )

                await self.aq.put(BookOfferRequest(
                    ledger_index = ledger_index,
                    taker_gets = taker_gets_currency,
                    taker_pays = taker_pays_currency,
                ))

        logger.info("[ExtractTokenPairsAsyncProcessor] Done iteration.")


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
        processors: List[FetchAsyncProcessor],
    ):
        self.url = url
        self.processors = processors
        self.id = cuid.cuid()

    async def _consume(self,
        aitr: AsyncIterator,
    ):
        async for entry in aitr:
            for proc in self.processors:
                await proc.process(entry)

    async def astart(self,
        request_execution: Callable[[AsyncWebsocketClient], Awaitable[None]],
    ):
        logger.info("[XRPLAsyncFetcher:%s] Start fetching.", self.id)
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
            logger.info("[LedgerIterationExecutioner] " +
                f"Current ledger index [{current_ledger_index}].")
            req = Ledger(
                ledger_index = current_ledger_index,
                transactions = True,
                expand = True,
            )
            await async_websocket_client.send(req)


BookOfferRequest = collections.namedtuple(
    "BookOfferRequest",
    ["ledger_index", "taker_gets", "taker_pays"],
)

class BookOfferRequestExecutioner:
    def __init__(self,
        bookoffer_req_queue: asyncio.Queue,
    ):
        self.bookoffer_req_queue = bookoffer_req_queue

    async def iterate_request(self,
        async_websocket_client: AsyncWebsocketClient,
    ):
        while True:
            # bookoffer_req_queue
            req = await bookoffer_req_queue.get()
            if type(req) != BookOfferRequest:
                logger.warn(
                    "expected 'BookOfferReqeuest' but encountered: '%s'",
                    type(req),
                )
                continue

            book_offer_request = BookOffers(
                taker_gets = req.taker_gets,
                taker_pays = req.taker_pays,
                ledger_index = req.ledger_index,
            )

            await req.request(book_offer_request)


async def amain():
    url = "wss://s1.ripple.com"

    bookoffer_req_queue = asyncio.Queue()


    ledger_index_queue = asyncio.Queue()
    ledger_index_queue.put_nowait(72650384)

    ledger_itr = LedgerIndexAsyncIterator(aqueue = ledger_index_queue)
    ledger_request_executioner = LedgerIterationExecutioner(ledger_itr)

    extract_token_pairs_processor = ExtractTokenPairsAsyncProcessor(
        aqueue = bookoffer_req_queue,
    )
    async_ledger_fetcher = XRPLAsyncFetcher(
        url = url,
        processors = [
            extract_token_pairs_processor,
        ],
    )
    ledger_fetcher_task = asyncio.create_task(
        async_ledger_fetcher.astart(
            request_execution = ledger_request_executioner.iterate_ledger_request,
        )
    )

    bookoffer_req_queue = asyncio.Queue()
    bookoffer_request_executioner = BookOfferRequestExecutioner(
        bookoffer_req_queue = bookoffer_req_queue,
    )
    async_bookoffer_fetcher = XRPLAsyncFetcher(
        url = url,
        processors = [],
    )
    bookoffer_fetcher_task = asyncio.create_task(
        async_bookoffer_fetcher.astart(
            request_execution = bookoffer_request_executioner.iterate_request,
        )
    )
    await asyncio.gather(
        *[ledger_fetcher_task, bookoffer_fetcher_task],
        return_exceptions = True,
    )


if __name__ == "__main__":
    asyncio.run(amain())
