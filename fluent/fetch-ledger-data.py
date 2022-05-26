import argparse
from attributes.collector import AttributeTypeMappingCollector
from collections import namedtuple
from etl.processor import \
    DictEntryProcessor, ETLTemplateDictEntryProcessor, GenericValidator, XRPLObjectTransformer, \
    STDOUTIngestor, FluentIngestor
from etl.schema import XRPLObjectSchema
from fluent import sender
from typing import Any, Dict, List
from xrpl.clients import WebsocketClient
from xrpl.models.requests.ledger_data import LedgerData
import asyncio
import json
import logging
import queue
import sys
import threading
import time


logger = logging.getLogger(__name__)
done_signal = namedtuple("done_signal", ['ledger_index'])


class XRPLedgerObjectFetcher:
    def __init__(self,
        url: str,
        processor: DictEntryProcessor,
    ):
        self.url = url
        self.processor = processor

    def start(self):
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
                    self.processor.process(obj)

                if not current_marker:
                    logger.info("[XRPLedgerObjectFetcher] No marker given. End of iteration.")
                    req = None
                    self.processor.done(
                        ledger_index = current_ledger_index,
                    )

                req = LedgerData(
                    ledger_index = current_ledger_index,
                    marker = current_marker,
                )

        logger.info("[XRPLedgerObjectFetcher] Finished exection.")


class EnqueueDictEntryProcessor(DictEntryProcessor):
    def __init__(self,
        entry_queue: queue.Queue,
    ):
        self.q = entry_queue

    def process(self,
        entry: Dict[str, Any]
    ):
        self.q.put(entry)

    def done(self, **kwargs):
        current_ledger_index = kwargs.get('ledger_index', 0)
        self.q.put(done_signal(
            ledger_index = current_ledger_index,
        ))


class DataAttributeMappingEntryProcessor(DictEntryProcessor):
    def __init__(self):
        self.att_mapping_collector = AttributeTypeMappingCollector()

    def process(self,
        entry: Dict[str, Any]
    ):
        self.att_mapping_collector.collect_attributes(entry)

    def done(self, **kwargs):
        # at the end, print the mapping collector to STDOUT
        self.att_mapping_collector.print()


class AggregateDictEntryProcessor(DictEntryProcessor):
    def __init__(self,
        entry_processors: List[DictEntryProcessor],
    ):
        self.entry_processors = entry_processors

    def process(self,
        entry: Dict[str, Any]
    ):
        for proc in self.entry_processors:
            proc.process(entry)

    def done(self, **kwargs):
        for proc in self.entry_processors:
            proc.done(**kwargs)


class DequeueProcessorTemplate():
    def __init__(self,
        entry_queue: queue.Queue,
        entry_processor: DictEntryProcessor,
    ):
        self.entry_queue = entry_queue
        self.entry_processor = entry_processor

    def start(self):
        while True:
            data_dict = self.entry_queue.get()

            if type(data_dict) == done_signal:
                # we got the done signal get out
                self.entry_queue.task_done()
                break

            if type(data_dict) != dict:
                logging.warn(
                    "Did not get 'dict' type instead got '%s' type.",
                    type(data_dict),
                )
                continue

            self.entry_processor.process(data_dict)

            self.entry_queue.task_done()

        logger.info("[QeueuAttachedFetchProcessorTemplate] Finished exection.")


def start_processors(
    wss_url: str,
    fluent_sender: sender.FluentSender,
    fluent_tag_name: str,
):

    if fluent_sender:
        ingestor = FluentIngestor(
            fluent_sender = fluent_sender,
            tag_name = fluent_tag_name,
        )
    else:
        ingestor = STDOUTIngestor()


    type_mapping_collector = AttributeTypeMappingCollector()
    att_mapping_entry_processor = DataAttributeMappingEntryProcessor()

    etl_template_processor = ETLTemplateDictEntryProcessor(
        validator = GenericValidator(XRPLObjectSchema.SCHEMA),
        transformer = XRPLObjectTransformer(),
        ingestor = ingestor,
    )

    agg_mapping_entry_processor = AggregateDictEntryProcessor(
        entry_processors = [
            att_mapping_entry_processor,
            etl_template_processor,
        ]
    )

    q = queue.Queue()

    enqueue_fetch_processor = EnqueueDictEntryProcessor(entry_queue = q)

    xrpl_fetcher = XRPLedgerObjectFetcher(
        url = wss_url,
        processor = enqueue_fetch_processor
    )
    fetcher_thread = threading.Thread(
        target = xrpl_fetcher.start,
    )
    fetcher_thread.daemon = True

    dequeue_processor_template = DequeueProcessorTemplate(
        entry_queue = q,
        entry_processor = agg_mapping_entry_processor,
    )
    processor_thread = threading.Thread(
        target = dequeue_processor_template.start,
    )
    processor_thread.daemon = True

    fetcher_thread.start()
    processor_thread.start()

    fetcher_thread.join()
    processor_thread.join()


def setup_stdout_logging():
    logging.basicConfig(
        stream = sys.stdout,  # set the out stream to STDOUT
        level = logging.INFO, # set level to INFO
    )


def main():
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument(
        "-f", "--fluent",
        help = "flag to turn on Fluent output",
        action="store_true",
    )
    arg_parser.add_argument(
        "-fh",
        "--fluent_host",
        help = "specify the FluentD/Bit host",
        type = str,
        default = "0.0.0.0",
    )
    arg_parser.add_argument(
        "-fp",
        "--fluent_port",
        help = "specify the FluentD/Bit port",
        type = int,
        default = 22522,
    )
    arg_parser.add_argument(
        "-xh",
        "--xrpl_host",
        help = "specify the rippled websocket host",
        type = str,
        default = "s2.ripple.com",
    )
    arg_parser.add_argument(
        "-xp",
        "--xrpl_port",
        help = "specify the rippled websocket port",
        type = str,
        default = "443",
    )
    arg_parser.add_argument(
        "-v",
        "--verbose",
        help = "turn on logging",
        action="store_true",
    )
    arg_parser.add_argument(
        "-env", "--environment",
        help = "specify the execution environment",
        default = "local",
        choices=["local", "test", "prod"],
    )
    arg_parser.add_argument(
        "-tag",
        "--tagname",
        help = "specify the fluent tag name",
        type = str,
        default = "ledger_objects",
    )
    cli_args = arg_parser.parse_args()

    if cli_args.verbose:
        setup_stdout_logging()

    fluent_sender = None
    if cli_args.fluent:
        fluent_sender = sender.FluentSender(
            cli_args.environment,
            host = cli_args.fluent_host,
            port = cli_args.fluent_port,
        )

    wss_url = f"wss://%s:%s/" % (cli_args.xrpl_host, cli_args.xrpl_port)

    logger.info(
        "Starting processors with WSS '%s' and On?[%r] FluentSender[%s:%d](env:'%s':tag:'%s') ",
        wss_url,
        cli_args.fluent,
        cli_args.fluent_host,
        cli_args.fluent_port,
        cli_args.environment,
        cli_args.tagname,
    )

    time.sleep(2)

    start_processors(
        wss_url = wss_url,
        fluent_sender = fluent_sender,
        fluent_tag_name = cli_args.tagname,
    )

if __name__ == "__main__":
    #start_processors()
    main()