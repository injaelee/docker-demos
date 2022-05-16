# create a network client
from xrpl.clients import JsonRpcClient
from xrpl.wallet import generate_faucet_wallet
import xrpl

def create_wallet(
	seed: str,
	sequence: int,
):
	return None


def setup_token(

):
	currency_code = "IJL"
	trust_set_tx = xrpl.models.transactions.TrustSet(
	    account=hot_wallet.classic_address,
	    limit_amount=xrpl.models.amounts.issued_currency_amount.IssuedCurrencyAmount(
	        currency=currency_code,
	        issuer=cold_wallet.classic_address,
	        value="10000000000", # Large limit, arbitrarily chosen
	    )
	)
	ts_prepared = xrpl.transaction.safe_sign_and_autofill_transaction(
	    transaction=trust_set_tx,
	    wallet=hot_wallet,
	    client=client,
	)
	print("Creating trust line from hot address to issuer...")
	response = xrpl.transaction.send_reliable_submission(ts_prepared, client)
	print(response)


def generate_new_wallet():
	# create the fake wallets
	client = JsonRpcClient("https://s.altnet.rippletest.net:51234/")
	wallet = generate_faucet_wallet(client)
	print("\n".join([
	    f"public_key     : {wallet.public_key}",
	    f"private_key    : {wallet.private_key}",
	    f"classic_address: {wallet.classic_address}",
	    f"seed           : {wallet.seed}",
	    f"sequence       : {wallet.sequence}",
    ]))


def setup_cold_address():
	return
"""
public_key     : EDF35B1E6E4440A01589BE6F1F6D8DE62C1FAC88C8EBB9DA0C9C846EC1296B9EC2
private_key    : ED4BA5886F3E998A66BE27431C7B67B621E75CB78AB992A410CDDCCC8BA68B46DB
classic_address: reFEaDcgzFAa8d1SSTjTP7DDmbUhbSzVr
seed           : sEdSLWCaNuTc7EopKxQJcN4Po9WWpJc
sequence       : 27747771
"""


def setup_for_all():
	testnet_url = "https://s.altnet.rippletest.net:51234"
	client = xrpl.clients.JsonRpcClient(testnet_url)

	# Get credentials from the Testnet Faucet --------------------------------------
	# For production, instead create a Wallet instance
	faucet_url = "https://faucet.altnet.rippletest.net/accounts"
	print("Getting 2 new accounts from the Testnet faucet...")
	from xrpl.wallet import generate_faucet_wallet
	cold_wallet = generate_faucet_wallet(client, debug=True)
	hot_wallet = generate_faucet_wallet(client, debug=True)


	# Configure issuer (cold address) settings -------------------------------------
	cold_settings_tx = xrpl.models.transactions.AccountSet(
	    account=cold_wallet.classic_address,
	    transfer_rate=0,
	    tick_size=5,
	    domain=bytes.hex("example.com".encode("ASCII")),
	    set_flag=xrpl.models.transactions.AccountSetFlag.ASF_DEFAULT_RIPPLE,
	)
	cst_prepared = xrpl.transaction.safe_sign_and_autofill_transaction(
	    transaction=cold_settings_tx,
	    wallet=cold_wallet,
	    client=client,
	)
	print("Sending cold address AccountSet transaction...")
	response = xrpl.transaction.send_reliable_submission(cst_prepared, client)
	print(response)


	# Configure hot address settings -----------------------------------------------
	hot_settings_tx = xrpl.models.transactions.AccountSet(
	    account=hot_wallet.classic_address,
	    set_flag=xrpl.models.transactions.AccountSetFlag.ASF_REQUIRE_AUTH,
	)
	hst_prepared = xrpl.transaction.safe_sign_and_autofill_transaction(
	    transaction=hot_settings_tx,
	    wallet=hot_wallet,
	    client=client,
	)
	print("Sending hot address AccountSet transaction...")
	response = xrpl.transaction.send_reliable_submission(hst_prepared, client)
	print(response)


	# Create trust line from hot to cold address -----------------------------------
	currency_code = "FOO"
	trust_set_tx = xrpl.models.transactions.TrustSet(
	    account=hot_wallet.classic_address,
	    limit_amount=xrpl.models.amounts.issued_currency_amount.IssuedCurrencyAmount(
	        currency=currency_code,
	        issuer="rsWx7fUGc5XvUM7nFr7D5JuCwGTY9u9LPQ", #cold_wallet.classic_address,
	        value="10000000000", # Large limit, arbitrarily chosen
	    )
	)
	ts_prepared = xrpl.transaction.safe_sign_and_autofill_transaction(
	    transaction=trust_set_tx,
	    wallet=hot_wallet,
	    client=client,
	)
	print("Creating trust line from hot address to issuer...")
	response = xrpl.transaction.send_reliable_submission(ts_prepared, client)
	print(response)


	# Send token -------------------------------------------------------------------
	issue_quantity = "3840"
	send_token_tx = xrpl.models.transactions.Payment(
	    account="rsWx7fUGc5XvUM7nFr7D5JuCwGTY9u9LPQ",#cold_wallet.classic_address,
	    destination=hot_wallet.classic_address,
	    amount=xrpl.models.amounts.issued_currency_amount.IssuedCurrencyAmount(
	        currency=currency_code,
	        issuer="rsWx7fUGc5XvUM7nFr7D5JuCwGTY9u9LPQ", #cold_wallet.classic_address,
	        value=issue_quantity
	    )
	)
	pay_prepared = xrpl.transaction.safe_sign_and_autofill_transaction(
	    transaction=send_token_tx,
	    wallet=cold_wallet,
	    client=client,
	)
	print(f"Sending {issue_quantity} {currency_code} to {hot_wallet.classic_address}...")
	response = xrpl.transaction.send_reliable_submission(pay_prepared, client)
	print(response)


def main():
	# create the fake wallets
	generate_new_wallet()
	# setup_for_all()


if __name__ == "__main__":
	main()