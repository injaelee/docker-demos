import xrpl
from fluent import sender, handler
from bson.objectid import ObjectId
from xrpl.models.requests import BookOffers, Ledger
from xrpl.models.transactions import Transaction, OfferCreate, Payment, OfferCancel
from xrpl.models.currencies import XRP, IssuedCurrency
import logging
import datetime
import sys
from typing import Dict, Any, Set, List
import json

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

class AttributeTypeMappingCollector:
    """
    Extracts and aggregates the key, data types pair given dictionaries.

    This is used to find the superset of the key to data types.
    Hierarchical keys are flatten with dots ".".

    Example) 
    start_key:
    |_a_key: v
    |_b_key: 
      |_c-key: []
    
    Results in:
      start_key            : <dict>
      start_key.a_key      : <str>
      start_key.b_key      : <dict>
      start_key.b_key.c_key: <list>
    """
    def __init__(self):
        self.attribute_mapping = {}

    def get_mapping(self) -> Dict[str, Any]:
        return self.attribute_mapping

    def collect_attributes(self,
        data_dict: Dict[str, Any],
    ):   
        self._collect_attributes(
            prefix = "",
            data_dict = data_dict,
            attribute_mapping = self.attribute_mapping,
        )

    def _collect_attributes(self,
        prefix: str,
        data_dict: Dict[str, Any],
        attribute_mapping: Dict[str, Set[str]],
    ):

        # extract out all the attributes
        for k, v in data_dict.items():

            attribute_name = prefix + k

            if type(v) == dict:
                self._collect_attributes(
                    prefix = attribute_name + ".",
                    data_dict = v,
                    attribute_mapping = attribute_mapping,
                )

            # assumption: there is no list of lists
            if type(v) == list:
                for entry in v:
                    if type(entry) == dict:
                        self._collect_attributes(
                            prefix = attribute_name + ".",
                            data_dict = entry,
                            attribute_mapping = attribute_mapping,
                        )

            if not attribute_mapping.get(attribute_name):
                attribute_mapping[attribute_name] = set()
            attribute_mapping[attribute_name].add(type(v))

    def print(self,
        delimeter = "\t",
    ):
        for k in sorted(self.attribute_mapping.keys()):
            v = self.attribute_mapping.get(k)
            print("{key}{delimeter}{value}".format(
                key = k, 
                delimeter = delimeter,
                value = v,
            ))

class XRPLAPIDemo:
    def __init__(self,
        xrpl_endpoint: str = "http://s1.ripple.com:51234/",
    ):
        self.xrpl_client = xrpl.clients.JsonRpcClient(xrpl_endpoint)

    def flatten_transaction_attribute_keys(self,
        ledger_indices: List[int],
    ):
        attribute_collector = AttributeTypeMappingCollector()
        for idx in ledger_indices:
            ledger_request = Ledger(
                ledger_index = idx,
                transactions = True, # include the individual transaction entries
                expand = True, # expand the transaction entries into actual data instead of references
            )

            # obtain the latest ledger index
            resp = self.xrpl_client.request(ledger_request)
            list_of_txns = resp.result.get("ledger", {}).get("transactions", [])    

            for txn in list_of_txns:

                txn_hash = txn.get("hash")
                del txn["hash"] # remove the hash key so we can convert back to an object

                # da_class = getattr(xrpl_models_txns_module, txn.get("TransactionType"))
                # o = da_class.from_xrpl(txn)
                # o = Transaction.from_xrpl(txn)
                # print(o)

                attribute_collector.collect_attributes(data_dict = txn)
        
        attribute_collector.print()
        
    def logging_ledger_info(self):
        """
        
        """
        ledger_request = Ledger(
            ledger_index = "current",
            transactions = True, # include the individual transaction entries
            expand = True, # expand the transaction entries into actual data instead of references
        )

        # obtain the latest ledger index
        resp = self.xrpl_client.request(ledger_request)
        list_of_txns = resp.result.get("ledger", {}).get("transactions", [])

        attribute_collector = AttributeTypeMappingCollector()
        for txn in list_of_txns:

            #print(json.dumps(txn, indent = 2))
            #tx = Transaction.from_xrpl(txn)
            print(txn.get("TransactionType", "N/A"))

            txn_hash = txn.get("hash")
            del txn["hash"] # remove the hash key so we can convert back to an object

            #da_class = getattr(xrpl_models_txns_module, txn.get("TransactionType"))
            # o = da_class.from_xrpl(txn)
            o = Transaction.from_xrpl(txn)
            print(o)

            attribute_collector.collect_attributes(data_dict = txn)
            
        attribute_collector.print()


    def logging_book_offers(self):
        wallet_address = "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
        issued_currency = IssuedCurrency(
            currency = "USD",
            issuer = "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B",
        )
        book_offer_request = BookOffers(
            taker = wallet_address,
            taker_gets = XRP(),
            taker_pays = issued_currency,
            ledger_index = "validated", # only get the validated data
        )

        resp = self.xrpl_client.request(book_offer_request)
        list_of_offers = resp.result.get("offers")
        attribute_collector = AttributeTypeMappingCollector()
        gcp_logger = logging.getLogger("gcp.table.integration_testing_book_offers")
        for offer in list_of_offers:
            attribute_collector.collect_attributes(data_dict = offer)
            gcp_logger.info(json.dumps(offer))

        attribute_collector.print()

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


def snippet_logging_illustration():
    """
    Illustrats direct streaming using the 
    'fluent.sender.FluentSender' which uses the
    'forward' protocol:
      https://docs.fluentbit.io/manual/pipeline/outputs/forward
    """
    # set up the fluent client
    logger = sender.FluentSender(
        "sample-logger-app", 
        host = "127.0.0.1", 
        port = 14225,
    )
    # the message to log
    # 
    structured_message = {
        "id": str(ObjectId()),
        "value": "Some value again at {event_date:%Y-%m-%d_%H:%M:%S.%f}".format(
            event_date = datetime.datetime.now(),
        ),
        "nested": {
            "level_1": {
                "level_2": "Event date is {event_date}".format(
                    event_date = datetime.datetime.now(),          
                )
            },
        },
        "nested_lists": [
            {
                "field_1": "Event date is 1: {event_date}".format(
                    event_date = datetime.datetime.now(),          
                ),
                "field_2": "Event date is 2: {event_date}".format(
                    event_date = datetime.datetime.now(),          
                ),
            }
        ],
    }
    """
    actual message looks like the following without new lines:
      in_msgpck_14225_sample-logger-app.some-tag-name: 
      [1651701828.000000000, {"id"=>"6272f8442eca0b1ec23ff05e", 
      "value"=>"Some value again at 2022-05-04_15:03:48.227155", 
      "nested"=>{"level_1"=>{"level_2"=>"Event date is 2022-05-04 15:03:48.227182"}}, 
      "nested_lists"=>[{"field_1"=>"Event date is 1: 2022-05-04 15:03:48.227189", 
      "field_2"=>"Event date is 2: 2022-05-04 15:03:48.227192"}]}]

    Here the "in_msgpck_14225_sample-logger-app.some-tag-name:" is the tag name.
    and the nextline is the actual content
    """
    
    # emit the dict as a stream entry
    # and get a refernece of the result
    r = logger.emit(
        "some-tag-name",
        structured_message,
    )

    # if there is a 'r', there was an error
    if not r:
        # check out the error
        print(logger.last_error)

    # must make sure the client is closed
    # for flushing the buffer and closing the
    # underlying socket
    logger.close()


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
    
    # test snippets
    #
    gcp_logger = logging.getLogger("gcp.table.the_table_name")

    structured_message = {
        "id": str(ObjectId()),
        "value": "Some value again.",
    }
    gcp_logger.info(json.dumps(structured_message))


if __name__ == "__main__":
    #snippet_logging_illustration()

    logger_setup(
        fluent_host = "0.0.0.0",
        fluent_port = 14225,
    )

    xrpl_demo = XRPLAPIDemo()
    #xrpl_demo.logging_ledger_info()
    
    xrpl_demo.flatten_transaction_attribute_keys(
        ledger_indices = [
          64936667,
          66462465,
          67188499,
          67790714,
          67849217,
          68828932,
          69389950,
          70328420,
          71327762,
          71381966,
          71428164,
          71442176,
          71450035,
          71452002,
          71453810,
          71453880,
          71453978,
          71453986,
          71454001,
        ]
    )
    

    #logging_integration()
    #xrpl_demo.logging_book_offers()
    xrpl_demo.demo_ledger_websocket()
    #snippet_logging_illustration()