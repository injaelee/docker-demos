import xrpl
from fluent import sender
from bson.objectid import ObjectId
from xrpl.models.requests import BookOffers
from xrpl.models.currencies import XRP, IssuedCurrency
import logging
import datetime

"""
gcp.bigquery.dataset.table_name

"""
class GCPBigQueryLoggingHandler(logging.Handler):
    def __init__(self,
        app_name: str,
        root_name: str,
        host: str,
        port: int,
        is_mock: bool = False,
    ):
        super(GCPBigQueryLoggingHandler, self).__init__()

        self.root_name = root_name
        self.host = host
        self.port = port
        self.app_name = app_name

        # the glue to having child logger names logged
        # ie. any suffixes that come after ${root_name}
        #     with a '.'(dot) will be logged
        self.addFilter(logging.Filter(name=root_name))

        self.fluent_logger = sender.FluentSender(
            app_name,
            host = host,
            port = port,
        )

        self.is_mock = is_mock

    def _emit(self,
        record: logging.LogRecord,
    ):
        if self.is_mock:
            print(tag, record.getMessage())
            return



    def emit(self,
        record: logging.LogRecord,
    ):
        # OUTPUT per table name 
        # dataset.tablename
        tag = record.name[len(self.rootName):]
        self._emit(
            tag,
            record.getMessage(),
        )

    def close(self):
        if not self.fluent_logger:
            return

        self.fluent_logger.close()


def logger_setup(
    fluent_host: str,
    fluent_port: int,
):
    gcp_table_logging_handler = GCPBigQueryLoggingHandler(
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
    not_gcp_logger = logging.getLogger("not.gcp.table")
    gcp_logger = logging.getLogger("gcp.table.the_table_name")

    not_gcp_logger.info("logging to a non GCP table")

    structured_message = {
        "id": str(ObjectId()),
        "value": "Some value again.",
    }
    gcp_logger.info(structured_message)


def logging_book_offers():
    wallet_address = "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
    print(XRP())
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
    print(book_offer_request)

    xrpl_client = xrpl.clients.JsonRpcClient("http://s1.ripple.com:51234/")
    r = xrpl_client.request(book_offer_request)
    print(r)
    """
    response = await client.request(
            BookOffers(
                taker = WALLET.classic_address,
                taker_gets = XRP(),
                taker_pays = IssuedCurrency(
                    currency = "USD",
                    issuer = "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B",
                ),
                ledger_index = "validated",
            ),
        )
    """

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


if __name__ == "__main__":
    snippet_logging_illustration()
    #logging_integration()
    #logging_book_offers()
    #logger_setup(
    #    fluent_host = "0.0.0.0",
    #    fluent_port = 14225,
    #)