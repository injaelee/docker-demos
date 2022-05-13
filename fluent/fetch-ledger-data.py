import json
from xrpl.clients import WebsocketClient
from xrpl.models.requests.ledger_data import LedgerData
import asyncio
from attributes.collector import AttributeTypeMappingCollector
import time
"""

What do we want?

for entry in data_puller.next():
    process(entry)

"""


class XRPLedgerObjectDataRepo:
    def __init__(self,
        url: str,
    ):
        self.url = url

    async def _start_fetch(self,
        entry_queue: asyncio.Queue,
    ):
        with WebsocketClient(self.url) as client:

            req = LedgerData(
                ledger_index = "current",
            )
            while req: # while there is a request keep fetching

                print("Making request: {}".format(req))
                response = await client.request_impl(req)

                print("Received Message.")

                if not response.is_successful():
                    # log error
                    # wait and retry again
                    import pdb; pdb.set_trace()
                    print("Received message has failure.")
                    await asyncio.sleep(10)
                    continue

                message = response.result

                current_ledger_index = message.get("ledger_index")
                current_marker = message.get("marker")

                list_of_ledger_objs = message.get('state')
                for obj in list_of_ledger_objs:
                    await entry_queue.put(obj)

                if not current_marker:
                    req = None
                    continue

                req = LedgerData(
                    ledger_index = current_ledger_index,
                    marker = current_marker,
                )

    def next(self):
        entry_queue: asyncio.Queue = asyncio.Queue()
        asyncio.run(self._start_fetch(entry_queue))

        while True:
            entry = entry_queue.get()
            yield entry
            entry_queue.task_done()


async def try_ledger_data(
    limit_iteration: int = -1,
):

    url = "wss://s1.ripple.com/"
    req = LedgerData(
        ledger_index = "current",
    )

    att_collector = AttributeTypeMappingCollector()

    iteration_count = 0
    message_count = 0
    start_ts = time.time()

    previous_key_count = 0

    # NOTE: this code will run forever without a timeout, until the process is killed
    with WebsocketClient(url) as client:
        while req and limit_iteration != 0: # while there is a request keep fetching
            limit_iteration -= 1

            print("Making request: {}".format(req))
            response = await client.request_impl(req)

            duration = time.time() - start_ts
            print("[{itr_num} :: {duration:.2f} s] Received Message.".format(
                itr_num = iteration_count,
                duration = duration,
            ))

            if not response.is_successful():
                # log error
                # wait and retry again
                print("[{itr_num}] Received message has failure. Sleeping.".format(itr_num = limit_iteration))
                await asyncio.sleep(10)
                continue

            iteration_count += 1
            message = response.result

            current_ledger_index = message.get("ledger_index")
            current_marker = message.get("marker")

            list_of_ledger_objs = message.get('state')
            for obj in list_of_ledger_objs:
                message_count += 1
                att_collector.collect_attributes(obj)

            # previous count comparison
            attribute_mapping = att_collector.get_mapping()
            if previous_key_count != len(attribute_mapping.keys()):
                att_collector.print()
                previous_key_count = len(attribute_mapping.keys())

            duration = time.time() - start_ts
            print("[{itr_num}:{duration:.2f}] Preparing next for ledger index '{ledger_index}' at marker '{marker}'.".format(
                itr_num = iteration_count,
                duration = duration,
                ledger_index = current_ledger_index,
                marker = current_marker,
            ))
            if not current_marker:
                import pdb; pdb.set_trace()
                req = None
                continue

            req = LedgerData(
                ledger_index = current_ledger_index,
                marker = current_marker,
            )

    duration = time.time() - start_ts
    print("======================================================== {}".format(duration))
    att_collector.print()


def main():
    data_repo = XRPLedgerObjectDataRepo(url = "wss://s1.ripple.com/")
    for entry in data_repo.next():
        print(entry)

"""
Let's try the ASYNC loop
"""

if __name__ == "__main__":
    #snippet_logging_illustration()
    asyncio.run(try_ledger_data())
    #main()