import argparse
import logging
import json
import pprint 


logger = logging.getLogger(__name__)
logging.basicConfig(level = logging.INFO)


def parse_arguments() -> argparse.Namespace:
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument(
        "-c",
        "--config",
        help = "specify config JSON file",
        type = str,
        required = True,
    )

    return arg_parser.parse_args()


def read_config(
    json_filepath:str,
):
    logger.info(
        "[read_config] reading '%s'.",
        json_filepath,
    )
    # open and read JSON file
    config_fh = open(json_filepath)
    config_dict = json.load(config_fh)

    # pretty print for readability
    logger.info(pprint.pformat(config_dict, indent = 4))


if __name__ == "__main__":
    args = parse_arguments()
    read_config(args.config)
