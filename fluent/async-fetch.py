from attributes.collector import AttributeTypeMappingCollector
from collections import namedtuple
from etl.processor import \
    DictEntryProcessor, ETLTemplateDictEntryProcessor, GenericValidator, XRPLObjectTransformer, \
    AggregateIngestor, STDOUTIngestor, FluentIngestor
from etl.schema import XRPLObjectSchema
from typing import Any, AsyncIterator, Dict, List, Set, Union
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.models import Subscribe, Unsubscribe, StreamParameter
from xrpl.models.requests.ledger_data import LedgerData
from xrpl.asyncio.ledger import get_latest_validated_ledger_sequence
import asyncio
from fluent import sender
import bson
import logging
import sys
import argparse

logger = logging.getLogger(__name__)
NextFetchInfo = namedtuple("NextFetchInfo", ["ledger_index", "marker"])
DoneSignal = namedtuple("DoneSignal", ["ledger_index"])
BaseURL = namedtuple("BaseURL", ["protocol", "host", "port"])

class AsyncXRPLedgerObjectFetcher:
    def __init__(self,
        wss_url: str,
        entry_processor: DictEntryProcessor,
        is_attach_execution_id: bool = True,
        is_attach_seq: bool = True,
        ledger_index: Union[int,str] = "current",
    ):
        self.wss_url = wss_url
        self.failure_count = 0

        self.entry_processor = entry_processor
        self.is_attach_execution_id = is_attach_execution_id
        self.is_attach_seq = is_attach_seq
        self.sequence = 0
        self.message_sequence = 0
        self.execution_id = str(bson.ObjectId())
        self.ledger_index = ledger_index

    async def _request_producer(self,
        client: AsyncWebsocketClient,
        marker_queue: asyncio.Queue,
    ):
        try:
            while True:
                next_fetch_info = await marker_queue.get()
                
                if type(next_fetch_info) == DoneSignal:
                    # done
                    logging.info(
                        "[_request_producer::RECV] Done signal index:[%s]", 
                        next_fetch_info.ledger_index,
                    )
                    return

                logging.debug(
                    "[_request_producer::RECV] index:[%s] marker:[%s]", 
                    next_fetch_info.ledger_index,
                    next_fetch_info.marker,
                )

                req = LedgerData(
                    ledger_index = next_fetch_info.ledger_index,
                    marker = next_fetch_info.marker,
                )
                await client.send(req)
        except Exception as exp:
            logger.error("[ERROR::_request_producer] %s", exp)
            return exp

    async def _message_processor(self,
        message_iterator: AsyncIterator,
        marker_queue: asyncio.Queue,
    ):
        # keep on consuming the message
        async for message in message_iterator:
            self.sequence += 1

            try:
                # check whether the message was successful and retry
                if message.get("status") != "success":
                    self.failure_count += 1
                    logging.error("[FAILED] Message: %s", message)
                    continue

                msgID = message.get('id')
                if msgID:
                    # this is the start of the loop
                    ledger_index = message.get('result').get('ledger').get('ledger_index')
                    await marker_queue.put(NextFetchInfo(
                        ledger_index = ledger_index,
                        marker = None
                    ))
                    continue

                result = message.get("result", {})
                ledger_index = result.get('ledger_index')
                marker = result.get('marker')
                list_of_ledger_objs = result.get('state')
                
                logging.info(
                    "[_message_processor::%d] index:[%s] marker:[%s]", 
                    self.sequence,
                    ledger_index,
                    marker,
                )

                # process the ledger obj's
                for ledger_obj in list_of_ledger_objs:
                    if self.is_attach_execution_id:
                        ledger_obj['_ExecutionID'] = self.execution_id

                    if self.is_attach_seq:
                        ledger_obj['_Sequence'] = self.message_sequence
                        self.message_sequence += 1

                    self.entry_processor.process(ledger_obj)

                if marker:
                    await marker_queue.put(NextFetchInfo(
                        ledger_index = ledger_index,
                        marker = marker,
                    ))
                else:
                    await marker_queue.put(DoneSignal(
                        ledger_index = ledger_index,
                    ))
                    return
            except Exception as exp:
                logger.error("[ERROR::_message_processor] %s", exp)
                return exp

    async def astart(self):
        marker_queue = asyncio.Queue()

        async with AsyncWebsocketClient(self.wss_url) as client:

            # create the mesage producer task
            request_producer_task = asyncio.create_task(
                self._request_producer(client, marker_queue),
            )

            # create the mesasage consuming task
            message_processor_task = asyncio.create_task(
                self._message_processor(client, marker_queue),
            )

            if self.ledger_index == "current":
                await get_latest_validated_ledger_sequence(client) - 1
                logger.info("[astart] Starting with current index")
            else:
                ledger_index = self.ledger_index
                logger.info("[astart] Starting with specific index %d", ledger_index)

                # start the process with the initial request
                await marker_queue.put(NextFetchInfo(
                    ledger_index = ledger_index,
                    marker = None,
                ))

            results = await asyncio.gather(
                request_producer_task, message_processor_task,
                return_exceptions = True,
            )

            # wait on the request producer task which 
            # processes the done signal
            await asyncio.sleep(10000)


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
            self.latest_key_count = len(self.att_mapping_collector.get_keys())
            print("-----")
            self.att_mapping_collector.print()
            print("-----")
            sys.stdout.flush()

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


def setup_stdout_logging():
    logging.basicConfig(
        stream = sys.stdout,  # set the out stream to STDOUT
        level = logging.INFO, # set level to INFO
    )


async def amain(
    base_url: BaseURL,
    is_stdout_ingestor: bool,
    fluent_sender: sender.FluentSender,
    fluent_tag_name: str,
):
    ingestors = []

    # if fluent sender is specified
    # forward outputs to fluent
    if fluent_sender:
        ingestors.append(FluentIngestor(
            fluent_sender = fluent_sender,
            tag_name = fluent_tag_name,
        ))
    
    if is_stdout_ingestor:
        # otherwise, output to STDOUT
        ingestors.append(STDOUTIngestor())

    if len(ingestors) == 1:
        ingestor = ingestors[0]
    else:
        ingestor = AggregateIngestor(ingestors = ingestors)

    # setup the entry processors
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

    async_ledger_fetcher = AsyncXRPLedgerObjectFetcher(
        wss_url = "wss://s1.ripple.com",
        entry_processor = agg_mapping_entry_processor,
    )
    await async_ledger_fetcher.astart()


if __name__ == "__main__":
    

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument(
        "-si", "--stdoutingest",
        help = "flag to turn on ingestor",
        action="store_true",
    )
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
        type = int,
        default = 443,
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

    wss_base_url = BaseURL(
        "wss", 
        cli_args.xrpl_host, 
        cli_args.xrpl_port,
    )
    asyncio.run(amain(
        base_url = wss_base_url,
        is_stdout_ingestor = cli_args.stdoutingest,
        fluent_sender = fluent_sender,
        fluent_tag_name = cli_args.tagname,
    ))
