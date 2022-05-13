from typing import Any, Dict, Set
import unittest

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
    def __init__(self,
        data_type_mapping: str = None,
    ):
        self.attribute_mapping = {}
        self.data_type_mapping = data_type_mapping

        # setup BigQuery Data Type
        self.bigquery_data_type_mapping = {
            int: "INTEGER",
            str: "STRING",
            dict: "RECORD",
        }

    def get_mapping(self) -> Dict[str, Any]:
        return self.attribute_mapping

    def _data_type_translation(self,
        data_type,
    ):
        if self.data_type_mapping == "google":
            return self.bigquery_data_type_mapping.get(
                data_type, 
                "NOT FOUND:[{}]".format(data_type),
            )

        return data_type


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

            data_type = self._data_type_translation(type(v))
            attribute_mapping[attribute_name].add(data_type)

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


class CollectorTestCases(unittest.TestCase):
    def test_good(self):
        structured_data_entries = [
            # 1
            {
                "dict-1": {
                    "boolean-n-string-1": True,
                    "string-1": "1D0BD042421251CAEA1F6D3C52B8F357FFDF90626A4738CC70DCD6291FFF1275",
                    "int-1": 0,
                    "float-1": 705695142.,
                },
            },
            # 1.1 with dupe types
            {
                "dict-1": {
                    "boolean-n-string-1": "string",
                    "string-1": "1D0BD042421251CAEA1F6D3C52B8F357FFDF90626A4738CC70DCD6291FFF1275",
                    "int-1": 0,
                    "float-1": 705695142.,
                },
            },
            # 2
            {
                "dict-2": {
                    "string-1": "SEC",
                    "list-1": [
                        {
                            "string-1": "string"
                        }
                    ]
                },
            },
        ]
        type_mapping_collector = AttributeTypeMappingCollector()
        for data_entry in structured_data_entries:
            type_mapping_collector.collect_attributes(data_entry)

        answer_dict = {
            "dict-1": {dict},
            "dict-1.boolean-n-string-1": {str, bool},
            "dict-1.float-1": {float},
            "dict-1.int-1": {int},
            "dict-1.string-1": {str},
            "dict-2": {dict},
            "dict-2.list-1": {list},
            "dict-2.list-1.string-1": {str},
            "dict-2.string-1": {str},
        }
        attribute_type_mapping = type_mapping_collector.get_mapping()
        for key, type_set in answer_dict.items():
            self.assertIsNotNone(attribute_type_mapping.get(key), "No key exists for key '{key}'".format(key = key))
            self.assertEqual(attribute_type_mapping.get(key), type_set, "Not equal for key '{key}'".format(key = key))

        # number of keys must match
        self.assertEqual(len(answer_dict), len(attribute_type_mapping))


if __name__ == "__main__":
    unittest.main()