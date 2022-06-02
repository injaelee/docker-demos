import asyncio
from collections import namedtuple
from typing import AsyncIterator
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.models import Subscribe, Unsubscribe, StreamParameter
from xrpl.models.requests.ledger_data import LedgerData

NextFetchInfo = namedtuple("NextFetchInfo", ["index", "marker"])

async def message_processor(
    client: AsyncIterator,
    marker_queue,
):
    async for message in client:
        ledger_index = message.get('result', {}).get('ledger_index')
        marker = message.get('result').get('marker')
        print(f"Index[{ledger_index}] Marker[{marker}]")
        await marker_queue.put(NextFetchInfo(
            index = ledger_index,
            marker = marker,
        ))

async def request_producer(
    client,
    marker_queue,
):
    while True:
        next_fetch_info = await marker_queue.get()
        req = LedgerData(
            ledger_index = next_fetch_info.index,
            marker = next_fetch_info.marker,
        )
        await client.send(req)


async def fetch_transaction_data():
    marker_queue = asyncio.Queue()

    url = "wss://s1.ripple.com"
    async with AsyncWebsocketClient(url) as client:

        asyncio.create_task(message_processor(client, marker_queue))
        asyncio.create_task(request_producer(client, marker_queue))

        req = LedgerData(
            ledger_index = 72050687,
        )
        await client.send(req)

        await asyncio.sleep(1000000)


if __name__ == "__main__":
    asyncio.run(fetch_transaction_data())