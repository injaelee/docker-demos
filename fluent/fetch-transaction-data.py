from typing import Any, Dict, List
from xrpl.clients import WebsocketClient
from xrpl.clients.json_rpc_client import JsonRpcClient
from xrpl.ledger import get_latest_validated_ledger_sequence
from xrpl.models.requests.ledger import Ledger
import argparse
import logging
import queue
import random
import threading
import time
import unittest


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


class NOOPDataCollector(DataCollector):
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
                account = step.get("account")

                if account:
                    cur = "rippling:" + account
                else:
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
                    traceback.print_exc()
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


def start_ledger_sequence() -> int:
    client = JsonRpcClient("https://s2.ripple.com:51234/")
    return get_latest_validated_ledger_sequence(client) - 1


def start_processors(
    start_index: int,
    concurrency_count: int,
):
    if start_index == -1:
        start_ladger_index = start_ledger_sequence()
    else:
        start_ladger_index = start_index

    shard_size = concurrency_count

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


class ProcessorTests(unittest.TestCase):
    def test_payment_fetch_processor(self):
        fetch_processor = PaymentFetchProcessor(NOOPDataCollector())
        parsed_paths = fetch_processor._parse_paths(
            paths = [
                [
                    {"account": "ACCOUNT_I", "type": 1},
                    {"account": "ACCOUNT_II", "type": 1},
                ],
                [
                    {"issuer": "ISSUER_I", "currency": "CUR", "type": 1},
                    {"currency": "XRP", "type": 1},
                ]
            ]
        )

        expected_paths = [
            ["rippling:ACCOUNT_I", "rippling:ACCOUNT_II"],
            ["CUR:ISSUER_I", "XRP"]
        ]

        for actual_path, expected_path in zip(parsed_paths, expected_paths):
            self.assertEqual(
                expected_path,
                actual_path,
                "paths are not equal",
            )

    def test_good_iterator(self):
        shard_index = 1
        shard_size = 5
        itr = ShardedLedgerIndexIterator(
            start_index = 100,
            shard_index = shard_index,
            shard_size = shard_size,
        )
        for i in itr:
            self.assertTrue(i % shard_size == shard_index)

    def test_next_iterator(self):
        shard_index = 1
        shard_size = 5
        itr = ShardedLedgerIndexIterator(
            start_index = 20,
            shard_index = shard_index,
            shard_size = shard_size,
        )
        expected_values = [16, 11, 6, 1]
        for actual_value, expected_value in zip(itr, expected_values):
            self.assertEqual(expected_value, actual_value, "unexpected sharded index")

        # test whether it is the end of the itr
        with self.assertRaises(StopIteration):
            next(itr)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument(
        "-x", "--execute",
        help = "execute the extraction",
        action="store_true",
    )
    arg_parser.add_argument(
        "-i", "--start_index",
        help = "ledger index start (index is iterated backwards toward 0)",\
        type = int,
        default = -1, # -1 means start from the latest
    )
    arg_parser.add_argument(
        "-n", "--thread_count",
        help = "number of threads to use",\
        type = int,
        default = 1,
    )

    cli_args = arg_parser.parse_args()

    # execute the unit test only
    if not cli_args.execute:
        unittest.main()


    start_processors(
        start_index = cli_args.start_index,
        concurrency_count = cli_args.thread_count,
    )

