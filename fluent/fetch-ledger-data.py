from attributes.collector import AttributeTypeMappingCollector
from collections import namedtuple
from etl.processor import \
    ETLProcessorTemplate, GenericValidator, XRPLObjectTransformer, STDOUTIngestor
from etl.schema import XRPLObjectSchema
from xrpl.clients import WebsocketClient
from xrpl.models.requests.ledger_data import LedgerData
import asyncio
import json
import logging
import queue
import threading
import time


logger = logging.getLogger(__name__)
done_signal = namedtuple("done_signal", ['ledger_index'])


class XRPLedgerObjectFetcher:
    def __init__(self,
        url: str,
    ):
        self.url = url

    def start_fetch(self,
        entry_queue: queue.Queue,
    ):
        with WebsocketClient(self.url) as client:

            req = LedgerData(
                ledger_index = "current",
            )
            itr_num = 0
            while req: # while there is a request keep fetching

                itr_num += 1
                response = client.request(req)
                if not response.is_successful():
                    logger.error(f"[{itr_num}] Received message has failure. Sleeping.")
                    time.sleep(10)
                    continue

                message = response.result

                current_ledger_index = message.get("ledger_index")
                current_marker = message.get("marker")

                list_of_ledger_objs = message.get('state')
                for obj in list_of_ledger_objs:
                    entry_queue.put(obj)

                if not current_marker:
                    req = None
                    entry_queue.put(done_signal(
                        ledger_index = current_ledger_index,
                    ))
                    continue

                req = LedgerData(
                    ledger_index = current_ledger_index,
                    marker = current_marker,
                )

        logger.info("[XRPLedgerObjectFetcher] Finished exection.")


class XRPLedgerETLProcessor:
    def __init__(self,
        etl_processor_template: ETLProcessorTemplate,
    ):
        self.etl_processor_template = etl_processor_template

    def process(self,
        entry_queue: asyncio.Queue,
    ):

        while True:
            data_dict = entry_queue.get()

            if type(data_dict) == done_signal:
                # we got the done signal get out
                entry_queue.task_done()
                break

            if type(data_dict) != dict:
                logging.warn(
                    "Did not get 'dict' type instead got '%s' type.",
                    type(data_dict),
                )
                continue

            self.etl_processor_template.process(data_dict)

            entry_queue.task_done()

        logger.info("[XRPLedgerETLProcessor] Finished exection.")


def start_processors():
    q = queue.Queue()

    xrpl_fetcher = XRPLedgerObjectFetcher(url = "wss://s1.ripple.com/")
    fetcher_thread = threading.Thread(
        target = xrpl_fetcher.start_fetch, args=(q,))
    fetcher_thread.daemon = True

    etl_processor = XRPLedgerETLProcessor(
        etl_processor_template = ETLProcessorTemplate(
            validator = GenericValidator(XRPLObjectSchema.SCHEMA),
            transformer = XRPLObjectTransformer(),
            ingestor = STDOUTIngestor(),
        ),
    )
    processor_thread = threading.Thread(
        target = etl_processor.process, args=(q,))
    processor_thread.daemon = True

    fetcher_thread.start()
    processor_thread.start()

    fetcher_thread.join()
    processor_thread.join()


if __name__ == "__main__":
    start_processors()