import _auto_run_with_pytest
from walytis_beta_api._experimental.generic_blockchain_testing import run_generic_blockchain_test
from walytis_beta_api import Blockchain
from walytis_mutability import MutaBlockchain


def test_generic_blockchain_features():

    blockchain = Blockchain.create()

    run_generic_blockchain_test(MutaBlockchain, base_blockchain=blockchain)
    blockchain.delete()

