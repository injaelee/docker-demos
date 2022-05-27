import argparse
import bson
from attributes.collector import AttributeTypeMappingCollector
from collections import namedtuple
from etl.processor import \
    DictEntryProcessor, ETLTemplateDictEntryProcessor, GenericValidator, XRPLObjectTransformer, \
    STDOUTIngestor, FluentIngestor
from etl.schema import XRPLObjectSchema
from fluent import sender
from typing import Any, Dict, List, Set, Union
from xrpl.clients import WebsocketClient
from xrpl.clients.json_rpc_client import JsonRpcClient
from xrpl.ledger import get_latest_validated_ledger_sequence
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
        is_attach_execution_id: bool = True,
        is_attach_seq: bool = True,
        ledger_index: Union[int,str] = "current",
    ):
        self.url = url
        self.processor = processor
        self.is_attach_execution_id = is_attach_execution_id
        self.is_attach_seq = is_attach_seq
        self.sequence = 0

        self.execution_id = str(bson.ObjectId())
        self.ledger_index = ledger_index

    def start(self):
        with WebsocketClient(self.url) as client:

            req = LedgerData(
                ledger_index = self.ledger_index,
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
                    obj['_LedgerIndex'] = int(current_ledger_index)

                    if self.is_attach_execution_id:
                        obj['_ExecutionID'] = self.execution_id

                    if self.is_attach_seq:
                        obj['_Sequence'] = self.sequence
                        self.sequence += 1

                    self.processor.process(obj)

                if not current_marker:
                    logger.info("[XRPLedgerObjectFetcher] No marker given. End of iteration.")
                    req = None
                    self.processor.done(
                        ledger_index = current_ledger_index,
                    )
                    continue

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
    def __init__(self,
        init_attribute_mapping: Dict[str, Set[str]] = None,
    ):
        self.att_mapping_collector = AttributeTypeMappingCollector(
            init_attribute_mapping = init_attribute_mapping,
        )
        self.latest_key_count = len(self.att_mapping_collector.get_keys())

    def process(self,
        entry: Dict[str, Any]
    ):
        self.att_mapping_collector.collect_attributes(entry)

        # print when a difference is detected
        if self.latest_key_count != len(self.att_mapping_collector.get_keys()):
            self.att_mapping_collector.print()

    def done(self, **kwargs):
        # do none
        pass


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
    buffer_size: int,
):

    # if fluent sender is specified
    # forward outputs to fluent
    if fluent_sender:
        ingestor = FluentIngestor(
            fluent_sender = fluent_sender,
            tag_name = fluent_tag_name,
        )
    else:
        # otherwise, output to STDOUT
        ingestor = STDOUTIngestor()


    type_mapping_collector = AttributeTypeMappingCollector()
    att_mapping_entry_processor = DataAttributeMappingEntryProcessor(
        init_attribute_mapping = XRPLObjectSchema.SCHEMA,
    )

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

    q = queue.Queue(maxsize = buffer_size)

    enqueue_fetch_processor = EnqueueDictEntryProcessor(entry_queue = q)

    xrpl_fetcher = XRPLedgerObjectFetcher(
        url = wss_url,
        processor = enqueue_fetch_processor,
        ledger_index = start_ledger_sequence(),
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


def start_ledger_sequence() -> int:
    client = JsonRpcClient("https://s2.ripple.com:51234/")
    return get_latest_validated_ledger_sequence(client) - 1


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
        default = "s1.ripple.com",
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
    arg_parser.add_argument(
        "-buf",
        "--buffer",
        help = "specify the buffer rows",
        type = int,
        default = 10000,
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
        "Starting processors with WSS '%s' and On?[%r] FluentSender[%s:%d](env:'%s':tag:'%s') ; buffer rows:[%d]",
        wss_url,
        cli_args.fluent,
        cli_args.fluent_host,
        cli_args.fluent_port,
        cli_args.environment,
        cli_args.tagname,
        cli_args.buffer,
    )

    time.sleep(2)

    start_processors(
        wss_url = wss_url,
        fluent_sender = fluent_sender,
        fluent_tag_name = cli_args.tagname,
        buffer_size = cli_args.buffer,
    )

if __name__ == "__main__":
    """
    # Common Ledger Data Operations
    ```bash
    python fetch-ledger-data.py -v -tag ledger_objects -env local \
    -xh s1.ripple.com -xp 443 \
    --fluent --fluent_host 0.0.0.0 --fluent_port 15225

    python fetch-ledger-data.py -v -tag ledger_objects -env local \
    -xh s1.ripple.com -xp 443

    python fetch-ledger-data.py -v -tag ledger_objects -env test \
    -xh s1.ripple.com -xp 443 \
    --fluent --fluent_host 0.0.0.0 --fluent_port 25225
    ```
    """
    main()