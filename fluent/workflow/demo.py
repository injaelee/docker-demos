from fluent import sender, handler
from numpy import random as nprandom
from prometheus_client import start_http_server, Histogram, Counter
from xrpl.asyncio.clients import AsyncJsonRpcClient, AsyncWebsocketClient
from xrpl.models.currencies import XRP, IssuedCurrency
from xrpl.models.requests import BookOffers, Ledger
from xrpl.asyncio.clients.utils import json_to_response, request_to_json_rpc
import asyncio
import logging
import xrpl
import traceback

class GCPBigQueryLoggingHandler(handler.FluentHandler):
    def __init__(self,
        root_name: str,
        *args, **kwargs
    ):
        self.root_name = root_name
        handler.FluentHandler.__init__(self, *args, **kwargs)

    def extract_tag_from(self,
        fullname: str,
    ) -> str:
        return fullname[len(self.root_name) + 1:]

    def emit(self,
        record: logging.LogRecord,
    ):
        fluent_tag = self.extract_tag_from(record.name)
        data = json.loads(record.getMessage())
        _sender = self.sender
        return _sender.emit_with_time(fluent_tag,
                                      sender.EventTime(record.created)
                                      if _sender.nanosecond_precision
                                      else int(record.created),
                                      data)

def logger_setup(
    fluent_host: str,
    fluent_port: int,
):
    # set up the custom logging handler
    gcp_table_logging_handler = GCPBigQueryLoggingHandler(
        tag = "gcp.table.prod",
        app_name = "prod.sample-logger-app",
        root_name = "gcp.table", # logger name prefix
        host = fluent_host,
        port = fluent_port,
    )
    gcp_table_logger = logging.getLogger("gcp.table")
    gcp_table_logger.propagate = False
    gcp_table_logger.setLevel("INFO")
    gcp_table_logger.addHandler(gcp_table_logging_handler)


def demo_ledger_websocket(self):
    from xrpl.clients import WebsocketClient
    url = "wss://s1.ripple.com/"
    from xrpl.models.requests import Subscribe, StreamParameter
    req = Subscribe(streams=[StreamParameter.LEDGER])
    # NOTE: this code will run forever without a timeout, until the process is killed
    with WebsocketClient(url) as client:
        client.send(req)
        for message in client:
            print(message)


async def fetch_transactions_demo_worker(
    worker_num: int,
    queue: asyncio.Queue,
    txn_histogram: Histogram,
):
    while True:
        with txn_histogram.time():
            sleep_time = nprandom.normal(0.4, 0.125)
            await asyncio.sleep(sleep_time)
            task = await queue.get()
            print(
                "[Worker:{}] Received - Fetching Transactions for: {}".format(
                worker_num,
                task.get("ledger_index", "empty"),
            ))
            queue.task_done()


"""
  Custom hack?
"""
from xrpl.models.requests.request import Request
from xrpl.models.response import Response
from httpx import AsyncClient

async def request_impl(url: str, request: Request) -> Response:
    async with AsyncClient(timeout = 10.0) as http_client:
        response = await http_client.post(
            url,
            json=request_to_json_rpc(request),
        )
        return json_to_response(response.json())


async def fetch_book_offers_worker(
    xrpl_client, 
    worker_num: int,
    queue: asyncio.Queue,
    txn_histogram: Histogram,
    logger,
):

    while True:
        with txn_histogram.time():
            task = await queue.get()

            ledger_index = task.get("ledger_index")
            if not ledger_index:
                print("[Worker:{}] Sleep to skip.".format(worker_num))
                await asyncio.sleep(5)
                continue

            wallet_address = "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
            issued_currency = IssuedCurrency(
                currency = "USD",
                issuer = "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B",
            )
            book_offer_request = BookOffers(
                taker = wallet_address,
                taker_gets = XRP(),
                taker_pays = issued_currency,
                ledger_index = ledger_index, # only get the validated data
            )

            print("[Worker:{}] Fetching book offers:{}".format(worker_num, ledger_index))

            try:
                resp = await xrpl_client.request_impl(book_offer_request)
                #resp = await request_impl(xrpl_client.url, book_offer_request)
            except:
                print(traceback.format_exc())
            
            list_of_offers = resp.result.get("offers", [])
            print("[Worker:{}] Got book offers [{}].".format(worker_num, len(list_of_offers)))
            #gcp_logger = logging.getLogger("gcp.table.integration_testing_book_offers")
            #for offer in list_of_offers:
            #    gcp_logger.info(json.dumps(offer))
            for offer in list_of_offers:

                r = logger.emit(
                    "gcp.table.integration_testing_book_offers",
                    offer,
                )
                # if there is a 'r', there was an error
                if not r:
                    # check out the error
                    print(logger.last_error)

            queue.task_done()


async def fetch_transactions_worker(
    xrpl_client, 
    worker_num: int,
    queue: asyncio.Queue,
    txn_histogram: Histogram,
):

    while True:
        print("[Worker:{}] Fetching task.".format(worker_num))
        task = await queue.get()
        ledger_index = task.get("ledger_index")
        if not ledger_index:
            print("[Worker:{}] Sleep to skip.".format(worker_num))
            await asyncio.sleep(5)
            continue

        ledger_request = Ledger(
            ledger_index = ledger_index,
            transactions = True, # include the individual transaction entries
            expand = True, # expand the transaction entries into actual data instead of references
        )

        print("[Worker:{}] Fetching ledger:{}".format(worker_num, ledger_index))
        try:
            resp = await request_impl(ledger_request) #await xrpl_client.request(ledger_request)
        except:
            print(traceback.format_exc())

        list_of_txns = resp.result.get("ledger", {}).get("transactions", [])    

        for txn in list_of_txns:
            print(txn)

        queue.task_done()



async def fetch_ledger_worker(
    queue: asyncio.Queue,
    ledger_counter: Counter,
):
    from xrpl.clients import WebsocketClient
    url = "wss://s1.ripple.com/"
    from xrpl.models.requests import Subscribe, StreamParameter
    req = Subscribe(streams=[StreamParameter.LEDGER])
    # NOTE: this code will run forever without a timeout, until the process is killed
    async with AsyncWebsocketClient(url) as client:
        await client.send(req)

        async for message in client:
            print("Put into queue: {} [{}]".format(message, queue.qsize()))
            await queue.put(message)
            ledger_counter.inc()


async def main(
    xrpl_client,
    ledger_counter: Counter,
    txn_histogram: Histogram,
    logger,
):
    async_q = asyncio.Queue()
    #tasks = [asyncio.create_task(fetch_transactions_worker(
    tasks = [asyncio.create_task(fetch_book_offers_worker(
                xrpl_client, i, async_q, txn_histogram, logger)) for i in range(4)]
    tasks += [asyncio.create_task(fetch_ledger_worker(async_q, ledger_counter))]
    await asyncio.wait(tasks)


import json
async def try_ledger_data():
    from xrpl.clients import WebsocketClient
    url = "wss://s1.ripple.com/"
    from xrpl.models.requests.ledger_data import LedgerData
    req = LedgerData()
    # NOTE: this code will run forever without a timeout, until the process is killed
    with WebsocketClient(url) as client:
        client.send(req)
        for message in client:
            print(json.dumps(message, indent=3))

if __name__ == "__main__":
    
    asyncio.run(try_ledger_data())


    logger_setup(
        fluent_host = "0.0.0.0",
        fluent_port = 14225,
    )

    # logging hack: just use the client directly
    logger = sender.FluentSender(
        "prod", 
        host = "127.0.0.1", 
        port = 14225,
    )

    xrpl_endpoint = "http://s1.ripple.com:51234/"
    xrpl_client = AsyncJsonRpcClient(xrpl_endpoint)

    start_http_server(8000)
    ledger_counter = Counter(
        "ledger_fetched", 
        "Number of ledgers fetched so far",
    )
    txn_histogram = Histogram(
        "txn_process_latency_seconds", 
        "Latency on processing the transactions",
    )
    try:
        asyncio.run(main(
            xrpl_client,
            ledger_counter,
            txn_histogram,
            logger,
        ))
    except KeyboardInterrupt:
        pass

    