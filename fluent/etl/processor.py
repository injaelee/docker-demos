from collections import namedtuple
from schema import XRPLTransactionSchema, XRPLObjectSchema
from typing import Dict, Any
import copy
import logging
import unittest

logger = logging.getLogger(__name__)

class Validator:

    def validate(self,
        data_entry: Dict[str, Any],
    ) -> Dict[str, Any]:
        return data_entry


class Transformer:

    def transform(self,
        data_entry: Dict[str, Any],
    ) -> Dict[str, Any]:
        return data_entry


class Ingestor:

    def ingest(self,
        data_entry: Dict[str, Any],
    ) -> Dict[str, Any]:
        return data_entry


class ETLProcessor:

    def __init__(self,
        validator,
        transformer,
        injestor,
    ):
        pass

    def process(self,
        data_entry: Dict[str, Any],
    ):
        
        validated_entry = self.validator(data_entry)

        transformed_entry = self.transformer.transform(data_entry)

        self.injestor.injest(transformed_entry)


class GenericValidator(Validator):
    def __init__(self,
        schema: Dict[str, Any],
    ):
        self.schema = schema

    def validate(self,
        data_entry: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Validate the data types
        """

        # make a deep copy to not touch the orginal
        working_data_entry = copy.deepcopy(data_entry)

        se = namedtuple("se", ["key", "entry_dict", "prefix"])
        keys_stack = []
        for key in working_data_entry.keys():
            keys_stack.append(se(key, working_data_entry, ""))

        while keys_stack:
            (current_key, entry_dict, key_prefix) = keys_stack.pop()

            full_key = (key_prefix + "." if key_prefix else "") + current_key

            allowed_data_type_set = self.schema.get(full_key, [])

            # check whether the key is defined within the schema
            # case sensitive
            if not allowed_data_type_set:
                logger.warning(
                    "removing key['%s'] that is not defined in the schema", 
                    full_key,
                )
                # remove the key
                del entry_dict[current_key]
                continue

            # check whether the data entry is allowed
            if type(entry_dict[current_key]) not in allowed_data_type_set:
                logger.warning(
                    "removing key['%s'] that is the wrong data type", 
                    full_key,
                )
                del entry_dict[current_key]
                continue

            if type(entry_dict[current_key]) == dict:
                current_dict = entry_dict[current_key]
                # push to the stack for evaluation
                for key in current_dict.keys():
                    keys_stack.append(se(key, current_dict, full_key))

        return working_data_entry


class XRPLObjectTransformer(Transformer):
    def transform(self,
        data_entry: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        For the dual types, ie. dict + str, consolidate
        to a single type, say, dict.

        "Balance": {str, dict},
        "SendMax": {str, dict},
        "TakerGets": {str, dict},
        "TakerPays": {str, dict},

        Convert these to dict.
        - currency
        - issuer
        - value
        """
        # make a deep copy to not touch the orginal
        working_data_entry = copy.deepcopy(data_entry)

        for k in ["Balance", "SendMax", "TakerGets", "TakerPays"]:
            
            if k not in working_data_entry:
                continue

            if type(working_data_entry.get(k)) is not dict:
                working_data_entry[k] = {
                    "currency": "XRP",
                    "issuer": "",
                    "value": data_entry[k],
                }

        return working_data_entry


class XRPLTransactionTransformer(Transformer):
    def transform(self,
        data_entry: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        "Amount": {str, dict},
        "SendMax": {str, dict},
        "TakerGets": {str, dict},
        "TakerPays": {str, dict},
        "metaData.AffectedNodes.CreatedNode.NewFields.Balance": {str, dict},
        "metaData.AffectedNodes.CreatedNode.NewFields.TakerGets": {str, dict},
        "metaData.AffectedNodes.CreatedNode.NewFields.TakerPays": {str, dict},
        "metaData.AffectedNodes.DeletedNode.FinalFields.Balance": {str, dict},
        "metaData.AffectedNodes.DeletedNode.FinalFields.TakerGets": {str, dict},
        "metaData.AffectedNodes.DeletedNode.FinalFields.TakerPays": {str, dict},
        "metaData.AffectedNodes.DeletedNode.PreviousFields.TakerGets": {str, dict},
        "metaData.AffectedNodes.DeletedNode.PreviousFields.TakerPays": {str, dict},
        "metaData.AffectedNodes.ModifiedNode.FinalFields.Balance": {str, dict},
        "metaData.AffectedNodes.ModifiedNode.FinalFields.TakerGets": {str, dict},
        "metaData.AffectedNodes.ModifiedNode.FinalFields.TakerPays": {str, dict},
        "metaData.AffectedNodes.ModifiedNode.PreviousFields.Balance": {str, dict},
        "metaData.AffectedNodes.ModifiedNode.PreviousFields.TakerGets": {str, dict},
        "metaData.AffectedNodes.ModifiedNode.PreviousFields.TakerPays": {str, dict},
        "metaData.DeliveredAmount": {str, dict},
        "metaData.delivered_amount": {str, dict},
        """
        working_data_entry = copy.deepcopy(data_entry)

        for k in ["Amount", "SendMax", "TakerGets", "TakerPays"]:
            
            if k not in working_data_entry:
                continue

            if type(working_data_entry.get(k)) is not dict:
                working_data_entry[k] = {
                    "currency": "XRP",
                    "issuer": "",
                    "value": data_entry[k],
                }

        meta_data_dict = working_data_entry.get("metaData", {})
        if not meta_data_dict:
            return working_data_entry

        for k in ["DeliveredAmount", "delivered_amount"]:
            if k not in meta_data_dict:
                continue

            if type(meta_data_dict[k]) is not dict:
                working_data_entry[k] = {
                    "currency": "XRP",
                    "issuer": "",
                    "value": meta_data_dict[k],
                }

        affected_nodes = meta_data_dict.get("AffectedNodes", [])
        if not affected_nodes:
            return working_data_entry


        for node_dict in affected_nodes:
            for k, kk in [
                ("CreatedNode", "NewFields"),
                ("DeletedNode", "FinalFields"),
                ("ModifiedNode", "FinalFields"),
                ("ModifiedNode", "PreviousFields"),
            ]:
                if k not in node_dict:
                    continue

                for kkk in ["Balance", "TakerGets", "TakerPays"]:
                    if kk not in node_dict[k] or kkk not in node_dict[k][kk]:
                        continue

                    if type(node_dict[k][kk][kkk]) != dict:
                        node_dict[k][kk][kkk] = {
                            "currency": "XRP",
                            "issuer": "",
                            "value": node_dict[k][kk][kkk],
                        }

        return working_data_entry


class ProcessorUnitTests(unittest.TestCase):

    def test_validator_ledgerobj(self):
        test_dict = {
            "TakerGets": {
                "currency": "value as dict",
                "issuer": "value as dict",
                "value": "value as dict",
                "notInSchema": "value not in schema",
            },
            "NotInTheSchema": "Hello Not Here",
            "SignerEntries": [
                "signer_01",
                "signer_02"
            ],
        }

        validator = GenericValidator(XRPLObjectSchema.SCHEMA)
        validated_dict = validator.validate(
            data_entry = test_dict,
        )

        # make sure that the one that is not in the schema is removed
        self.assertIsNotNone(test_dict.get("TakerGets", {}).get("notInSchema"))
        self.assertIsNone(validated_dict.get("TakerGets", {}).get("notInSchema"))

        # same here
        self.assertIsNotNone(test_dict.get("NotInTheSchema"))
        self.assertIsNone(validated_dict.get("NotInTheSchema"))

        # check the values of those that remain after the validation
        for key in ["currency", "issuer", "value"]:
            self.assertEqual(
                test_dict.get("TakerGets", {}).get(key),
                validated_dict.get("TakerGets", {}).get(key),
            )

        # check whether the values that remain
        self.assertEqual(
            len(test_dict.get("SignerEntries")), 
            len(validated_dict.get("SignerEntries")),
        )
        for expected, actual in zip(
            test_dict.get("SignerEntries"),
            validated_dict.get("SignerEntries"),
        ):
            self.assertEqual(expected, actual)

        # there should be only two keys available in the validated
        self.assertEqual(2, len(validated_dict))

    def test_transformer_ledgerobj(self):
        test_dict = {
            "TakerGets": {
                "notInSchema": "value not in schema",
                "currency": "value as dict1",
                "issuer": "value as dict2",
                "value": "value as dict3",
            },
            "Balance": "1000002",
            "SendMax": {
                "currency": "value as dict4",
                "issuer": "value as dict5",
                "value": "value as dict6",
            },
            "TakerPays": "101010"
        }
        
        transformer = XRPLObjectTransformer()
        transformed_dict = transformer.transform(test_dict)
        
        for k in ["TakerGets", "SendMax", "TakerGets"]:
            for mk in ["currency", "issuer", "value"]:
                self.assertEqual(
                    test_dict.get(k, {}).get(mk),
                    transformed_dict.get(k, {}).get(mk),
                )

        self.assertIsNotNone(
            transformed_dict.get("TakerGets",{}).get("notInSchema")
        )

        for k in ["Balance", "TakerPays"]:
            self.assertEqual(
                test_dict.get(k, None),
                transformed_dict.get(k, {}).get("value"),
            )
            self.assertEqual(
                "",
                transformed_dict.get(k, {}).get("issuer"),
            )
            self.assertEqual(
                "XRP",
                transformed_dict.get(k, {}).get("currency"),
            )

    def test_transformer_txns(self):
        test_dict = {
            "metaData": {
                "AffectedNodes": [
                    {
                        "ModifiedNode": {
                          "FinalFields": {
                            "Balance": "203412878",
                          },
                          "LedgerEntryType": "AccountRoot",
                          "LedgerIndex": "9810BE74435EB83B313E6B5C6E475CF5CAA3A0BED92AA9BD7706A6ADAB0260EA",
                          "PreviousFields": {
                            "Balance": "203412898",
                          },
                          "PreviousTxnID": "136C6C3FD366160B89A11601133E70908ADE17B4EA69A6E9E2DC6865486AEE84",
                          "PreviousTxnLgrSeq": 71698271
                        }
                      },
                ]
        }  }
        transformer = XRPLTransactionTransformer()
        transformed_dict = transformer.transform(test_dict)

        #print(transformed_dict)
        import json
        print(json.dumps(transformed_dict, indent = 4))


if __name__ == "__main__":
    unittest.main()
