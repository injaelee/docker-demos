from typing import Any, Dict, List
from xrpl.clients import WebsocketClient
from xrpl.models.requests.ledger import Ledger
from xrpl.clients.json_rpc_client import JsonRpcClient
from xrpl.ledger import get_latest_validated_ledger_sequence
import queue
import threading
import time
import unittest
import logging
import random

logger = logging.getLogger(__name__)

class FetchProcessor:
    def __init__(serf,
    ):
        pass

    def process(self,
        entry: Dict[str, Any]
    ):
        pass


class DataCollector:
    def collect(self,
        data: str,
    ):
        pass


class OutputQueueDataCollector(DataCollector):
    def __init__(self,
        entry_queue: queue.Queue,
    ):
        self.entry_queue = entry_queue

    def collect(self,
        data: str,
    ):
        self.entry_queue.put(data)


class STDOutDataCollector(DataCollector):
    def collect(self,
        data: str,
    ):
        print(data)


class PaymentFetchProcessor(FetchProcessor):
    def __init__(self,
        data_collector: DataCollector,
    ):
        self.data_collector = data_collector

    def _parse_paths(self,
        paths: List[List[Dict[str,str]]],
    ) -> List[List[str]]:
        path_list = []
        for path in paths:
            resolved_path = []
            for step in path:
                currency = step.get("currency")
                issuer = step.get("issuer")

                cur = currency + ":" + issuer if issuer else currency
                resolved_path.append(cur)
            path_list.append(resolved_path)
        return path_list

    def process(self,
        entry: Dict[str, Any],
        **kwargs,
    ):
        if entry.get("TransactionType") != "Payment":
            return

        offer_count = 1
        path_size = 0

        step_sizes = []
        path_list = self._parse_paths(entry.get("Paths", []))
        path_size = len(path_list)
        paths_str = ""
        for steps in path_list:
            if paths_str:
                paths_str += "-"
            step_sizes.append(str(len(steps)))
            paths_str += ">".join(steps)

        for affected_node in entry.get("metaData", {}).get("AffectedNodes", []):
            if affected_node.get("ModifiedNode", {}).get("LedgerEntryType") == "Offer":
                offer_count += 1

        # obtain the transaction status
        txn_result = entry.get("metaData", {}).get("TransactionResult")

        ledger_index = kwargs.get("ledger_index")
        txn_hash = entry.get("hash")

        step_sizes_str = "|" + "\t".join(step_sizes) if len(step_sizes) > 0 else ""
        self.data_collector.collect(
            f"{ledger_index}\t{txn_hash}\t{txn_result}\t{path_size}\t{offer_count}{step_sizes_str}\t*{paths_str}")


class OutputQueueProcessor:
    def process(self,
        entry_queue: queue.Queue,
    ):
        while True:
            entry = entry_queue.get()
            print(entry)
            entry_queue.task_done()


class ShardedLedgerIndexIterator:
    def __init__(self,
        start_index: int,
        shard_index: int,
        shard_size: int,
        incr_by: int = -1,
    ):
        self.shard_index = shard_index
        self.shard_size = shard_size
        self.current_index = start_index
        self.incr_by = incr_by

    def __iter__(self):
        return self

    def __next__(self):

        while self.current_index > 0:

            if self.current_index % self.shard_size == self.shard_index:
                idx = self.current_index
                self.current_index += self.incr_by
                return idx

            self.current_index += self.incr_by

        raise StopIteration


class XRPLedgerFetcherRPC:
    def __init__(self,
        url: str,
    ):
        self.url = url

    def start_fetch(self,
        next_ledger_index_itr: ShardedLedgerIndexIterator,
        processors: List[FetchProcessor],
    ):
        time.sleep(random.randrange(0,60)) # stagger start
        logger.info("[XRPLedgerFetcherRPC] Start fetching.")

        client = JsonRpcClient(self.url)

        for current_ledger_index in next_ledger_index_itr:

            retry = 0
            while retry < 5:
                try:
                    itr_num = 0

                    req = Ledger(
                        ledger_index = current_ledger_index,
                        transactions = True,
                        expand = True,
                    )

                    itr_num += 1
                    response = client.request(req)
                    if not response.is_successful():
                        raise RuntimeError("Response was not successful.")
                        continue

                    message = response.result

                    txns = message.get("ledger").get("transactions")
                    ledger_index = message.get("ledger_index")

                    for txn in txns:
                        for proc in processors:
                            proc.process(
                                txn,
                                ledger_index = ledger_index,
                            )

                    # break from the retry loop
                except Exception as e:
                    retry += 1
                    sleep_time_s = 10 * (1.5) ** retry + random.randrange(2,8)
                    logger.error(f"[{itr_num}] Received message has failure: {e}")
                    logger.warn(f"Now sleeping for {sleep_time_s} seconds.")
                    time.sleep(sleep_time_s)
                    continue

                # break retry loop when successful
                break

        logger.info("[XRPLedgerFetcherRPC] Finished exection.")


class XRPLStatisticCollector:
    def __init__(self):
        pass

    def collect(self,
        entry_queue: queue.Queue,
    ):
        return


class TestIterator(unittest.TestCase):

    def test_good(self):
        shard_index = 1
        shard_size = 5
        itr = ShardedLedgerIndexIterator(
            start_index = 100,
            shard_index = shard_index,
            shard_size = shard_size,
        )
        for i in itr:
            self.assertTrue(i % shard_size == shard_index)

    def test_next(self):
        shard_index = 1
        shard_size = 5
        itr = ShardedLedgerIndexIterator(
            start_index = 10,
            shard_index = shard_index,
            shard_size = shard_size,
        )
        for i in range(20):
            print(next(itr))


def start_ledger_sequence() -> int:
    client = JsonRpcClient("https://s2.ripple.com:51234/")
    return 62715437 # get_latest_validated_ledger_sequence(client) - 1


def single_threaded_start():
    start_ladger_index = start_ledger_sequence()
    ledger_index_iter = ShardedLedgerIndexIterator(
        start_index = start_ladger_index,
        shard_index = 0,
        shard_size = 1,
    )
    output_collector = STDOutDataCollector()    
    pymnt_fetch_processor = PaymentFetchProcessor(output_collector)
    fetch_processors = [pymnt_fetch_processor]

    xrpl_fetcher = XRPLedgerFetcherRPC(url = "https://s2.ripple.com:51234/") #(url = "wss://s2.ripple.com/")
    xrpl_fetcher.start_fetch(
        ledger_index_iter,
        fetch_processors,
    )


def start_processors():
    # 71840265 has the "Paths"
    # other use 'get_latest_validated_ledger_sequence'
    start_ladger_index = start_ledger_sequence()
    shard_size = 5

    output_queue = queue.Queue()
    output_collector = OutputQueueDataCollector(output_queue)

    pymnt_fetch_processor = PaymentFetchProcessor(output_collector)
    fetch_processors = [pymnt_fetch_processor]

    fetcher_threads = []
    for i in range(shard_size):
        ledger_index_iter = ShardedLedgerIndexIterator(
            start_index = start_ladger_index,
            shard_index = i,
            shard_size = shard_size,
        )
        xrpl_fetcher = XRPLedgerFetcherRPC(url = "https://s2.ripple.com:51234/") #(url = "wss://s2.ripple.com/")
        fetcher_thread = threading.Thread(
            target = xrpl_fetcher.start_fetch,
            args = (ledger_index_iter, fetch_processors,))
        fetcher_thread.daemon = True
        fetcher_thread.start()
        fetcher_threads.append(fetcher_thread)

    output_queue_processor = OutputQueueProcessor()
    output_queue_thread = threading.Thread(
        target = output_queue_processor.process,
        args = (output_queue,)
    )
    output_queue_thread.daemon = True
    output_queue_thread.start()
        
    for fetcher_thread in fetcher_threads:
        fetcher_thread.join()

    while not output_queue.empty():
        time.sleep(1)


if __name__ == "__main__":
    #start_processors()
    single_threaded_start()
    #unittest.main()