# create a network client
from xrpl.clients import JsonRpcClient
from xrpl.wallet import generate_faucet_wallet
from xrpl.models.amounts.issued_currency_amount import IssuedCurrencyAmount
from xrpl.models.transactions import OfferCreate
import xrpl
from collections import namedtuple
from typing import Union

# ------------------------------------------------------------------------------------------------- #
#                                                                                                   #
# ------------------------------------------------------------------------------------------------- #

WalletSecret = namedtuple("WalletSecret", ["seed", "sequence"])

class WalletBuilder:
    def __init__(self):
        self.wallet_secret: WalletSecret = None
        self.xrpl_client = None

    def with_wallet_secret(self,
        wallet_secret: WalletSecret,
    ):
        self.wallet_secret = wallet_secret
        return self

    def with_xrpl_client(self,
        xrpl_client: JsonRpcClient,
    ):
        self.xrpl_client = xrpl_client
        return self

    def build(self) -> xrpl.wallet.main.Wallet:
        if self.wallet_secret:
            return xrpl.wallet.main.Wallet(
                seed = self.wallet_secret.seed,
                sequence = self.wallet_secret.sequence,
            )

        if not self.xrpl_client:
            raise RuntimeError("Missing XRPL Client to build a new wallet.")

        return generate_faucet_wallet(self.xrpl_client, debug=True)

# ------------------------------------------------------------------------------------------------- #
#                                                                                                   #
# ------------------------------------------------------------------------------------------------- #

class TokenAmountBuilder:
    TOKEN_ISSUER_MAPPING = {
        "IJL": "rJ248EQck3oH1bQWRWfpZugoWSgTPgJW5V",
    }

    @classmethod
    def build_amount(cls,
        token_name: str,
        token_amount: str,
    ) -> Union[str,IssuedCurrencyAmount]:

        if token_name == "XRP":
            return token_amount

        issuer = cls.TOKEN_ISSUER_MAPPING.get(token_name)
        return IssuedCurrencyAmount(
            currency = token_name,
            issuer = issuer,
            value = token_amount,
        )

# ------------------------------------------------------------------------------------------------- #
#                                                                                                   #
# ------------------------------------------------------------------------------------------------- #

import cmd
class DEXShell(cmd.Cmd):
    def __init__(self,
        xrpl_url: str = "https://s.altnet.rippletest.net:51234",
    ):
        super(DEXShell, self).__init__()
        self.xrpl_client = xrpl.clients.JsonRpcClient(xrpl_url)
        self.named_wallet = {}

    def do_build_wallet(self,
        arg: str,
    ):
        """wallet [name] [seed] [sequence]"""
        print(f"[setwallet] arg: '{arg}'")
        arg_values = [entry for entry in arg.split()]

        name = seed = sequence = None
        if len(arg_values) == 1:
            name = arg_values[0]
        else:
            name, seed, sequence = arg_values

        wallet_builder = WalletBuilder()

        if seed and sequence:
            wallet_builder.with_wallet_secret(WalletSecret(
                seed = seed,
                sequence = sequence,
            ))

        wallet_builder.with_xrpl_client(
            self.xrpl_client,
        )

        wallet = wallet_builder.build()
        print(f"[Setting Wallet '{name}']:\n{wallet}")
        self.named_wallet[name] = wallet

    def do_get_wallet_info(self,
        arg: str,
    ):
        print(f"[get_wallet_info] arg: '{arg}'")
        wallet_name = arg
        wallet = self.named_wallet[wallet_name]

        if wallet:
            print(wallet)
            print(f"seed: {wallet.seed}")
            print(f"seq : {wallet.sequence}")
        else:
            print(f"No wallet found with name '{wallet_name}'.")

    def do_setup_cold_wallet(self,
        arg: str,
    ):
        """setup_cold_wallet [wallet name] [domain name]"""

        print(f"[setup_cold_wallet] arg: '{arg}'")

        arg_values = arg.split()
        if len(arg_values) != 2:
            print(f"[setup_cold_wallet] Missing arguments. Got {arg_values}")
            return

        wallet_name, domain_name = arg_values

        wallet = self.named_wallet.get(wallet_name)
        if not wallet:
            print(f"Wallet named '{wallet_name}' does not exist.")
            return

        cold_settings_tx = xrpl.models.transactions.AccountSet(
            account = wallet.classic_address,
            transfer_rate = 0,
            tick_size = 5,
            domain = bytes.hex(domain_name.encode("ASCII")),
            set_flag = xrpl.models.transactions.AccountSetFlag.ASF_DEFAULT_RIPPLE,
        )
        cst_prepared = xrpl.transaction.safe_sign_and_autofill_transaction(
            transaction = cold_settings_tx,
            wallet = wallet,
            client = self.xrpl_client,
        )
        print("[setup_cold_wallet] Sending cold address AccountSet transaction...")
        response = xrpl.transaction.send_reliable_submission(
            cst_prepared,
            self.xrpl_client,
        )
        print(response)

    def do_setup_hot_wallet(self,
        arg: str,
    ):
        """setup_hot_wallet [wallet name]"""

        wallet_name = arg

        wallet = self.named_wallet.get(wallet_name)
        if not wallet:
            print(f"[setup_hot_wallet] Wallet named '{wallet_name}' does not exist.")
            return

        hot_settings_tx = xrpl.models.transactions.AccountSet(
            account = wallet.classic_address,
            set_flag = xrpl.models.transactions.AccountSetFlag.ASF_REQUIRE_AUTH,
        )
        hst_prepared = xrpl.transaction.safe_sign_and_autofill_transaction(
            transaction = hot_settings_tx,
            wallet = wallet,
            client = self.xrpl_client,
        )
        print("[setup_hot_wallet] Sending hot address AccountSet transaction...")
        response = xrpl.transaction.send_reliable_submission(
            hst_prepared,
            self.xrpl_client,
        )
        print(response)

    def do_setup_token(self,
        arg: str,
    ):
        """setup_token [token name] [token amount] [receving wallet name] [issuer wallet name]"""

        print(f"[setup_token] arg: '{arg}'")
        arg_values = [entry for entry in arg.split()]

        if len(arg_values) != 4:
            print(f"[setup_token] Missing arguments. Got {arg_values}")
            return

        token_name, token_amount, receiving_wallet_name, issuer_wallet_name = arg_values

        token_name = token_name.upper()
        token_amount = int(token_amount)

        receiving_wallet = self.named_wallet.get(receiving_wallet_name)
        issuer_wallet = self.named_wallet.get(issuer_wallet_name)

        if not issuer_wallet:
            print(f"[setup_token] Issuer wallet '{issuer_wallet_name}' not found.")
            return

        if not receiving_wallet:
            print(f"[setup_token] Receiving wallet '{receiving_wallet_name}' not found.")
            return

        trust_set_tx = xrpl.models.transactions.TrustSet(
            account = receiving_wallet.classic_address,
            limit_amount = IssuedCurrencyAmount(
                currency = token_name,
                issuer = issuer_wallet.classic_address,
                value = token_amount, # Large limit, arbitrarily chosen
            )
        )
        ts_prepared = xrpl.transaction.safe_sign_and_autofill_transaction(
            transaction = trust_set_tx,
            wallet = receiving_wallet,
            client = self.xrpl_client,
        )
        print("[setup token] Creating trust line from hot address " +
            f"'{receiving_wallet_name}' to issuer '{issuer_wallet_name}'...")
        response = xrpl.transaction.send_reliable_submission(
            ts_prepared,
            self.xrpl_client,
        )
        print(response)

    def do_issue_token(self,
        arg: str,
    ):
        """issue_token [token name] [token amount] [issuer wallet name] [receiver wallet name]"""

        print(f"[issue_token] line: '{arg}'")

        arg_values = [entry for entry in arg.split()]

        if len(arg_values) != 4:
            print(f"[issue_token] Missing arguments. Got {arg_values}")
            return

        token_name, token_amount, issuer_wallet_name, receiving_wallet_name = arg_values

        token_name = token_name.upper()
        token_amount = int(token_amount)

        receiving_wallet = self.named_wallet.get(receiving_wallet_name)
        issuer_wallet = self.named_wallet.get(issuer_wallet_name)

        if not issuer_wallet:
            print(f"[issue_token] Issuer wallet '{issuer_wallet_name}' not found.")
            return

        if not receiving_wallet:
            print(f"[issue_token] Receiving wallet '{receiving_wallet_name}' not found.")
            return

        send_token_tx = xrpl.models.transactions.Payment(
            account = issuer_wallet.classic_address,
            destination = receiving_wallet.classic_address,
            amount = IssuedCurrencyAmount(
                currency = token_name,
                issuer = issuer_wallet.classic_address,
                value = token_amount
            )
        )
        pay_prepared = xrpl.transaction.safe_sign_and_autofill_transaction(
            transaction = send_token_tx,
            wallet = issuer_wallet,
            client = self.xrpl_client,
        )
        print(f"[issue_token] Sending {token_amount} {token_name} to wallet '{receiving_wallet_name}'...")
        response = xrpl.transaction.send_reliable_submission(
            pay_prepared,
            self.xrpl_client,
        )
        print(response)

    def do_offer_create(self,
        arg: str,
    ):
        """offer_create [wallet_name] [sell_quantity]:[sell_token_name] [buy_quantity]:[buy_token_name]"""
        arg_values = [entry for entry in arg.split()]

        if len(arg_values) != 3:
            print(f"[offer_create] Missing arguments. Got {arg_values}")
            return

        wallet_name, sell_order, buy_order = arg_values
        sell_quantity, sell_token = sell_order.split(":")
        buy_quantity, buy_token = buy_order.split(":")

        account_wallet = self.named_wallet.get(wallet_name)

        offer_create_tx = OfferCreate(
            account = account_wallet.classic_address,
            taker_gets = TokenAmountBuilder.build_amount(
                token_name = sell_token,
                token_amount = sell_quantity,
            ),
            taker_pays = TokenAmountBuilder.build_amount(
                token_name = buy_token,
                token_amount = buy_quantity,
            ),
        )
        print(f"[offer_create] Sending offer to buy {buy_order} for {sell_order}...")
        offer_prepared = xrpl.transaction.safe_sign_and_autofill_transaction(
            transaction = offer_create_tx,
            wallet = account_wallet,
            client = self.xrpl_client,
        )
        response = xrpl.transaction.send_reliable_submission(
            offer_prepared,
            self.xrpl_client,
        )
        print(response)

    def do_quit(self,
        arg: str,
    ):
        return True


def main():
    # create the fake wallets
    # generate_new_wallet()
    # setup_for_all()
    DEXShell().cmdloop()


if __name__ == "__main__":
    main()