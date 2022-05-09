from prometheus_client import start_http_server, Histogram, Counter
import asyncio
from numpy import random as nprandom
import xrpl
from xrpl.asyncio.clients import AsyncJsonRpcClient, AsyncWebsocketClient
from xrpl.models.requests import Ledger


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
        resp = await xrpl_client.request(ledger_request)
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
            await asyncio.sleep(1)


async def main(
    xrpl_client,
    ledger_counter: Counter,
    txn_histogram: Histogram,
):
    async_q = asyncio.Queue()
    tasks = [asyncio.create_task(fetch_transactions_worker(
        xrpl_client, i, async_q, txn_histogram)) for i in range(4)]
    tasks += [asyncio.create_task(fetch_ledger_worker(async_q, ledger_counter))]
    await asyncio.wait(tasks)



if __name__ == "__main__":
    
    
    xrpl_endpoint = "http://s1.ripple.com:51234/",
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
        ))
    except KeyboardInterrupt:
        pass

    