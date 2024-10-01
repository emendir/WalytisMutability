from walytis_beta_api.generic_blockchain_testing import test_generic_blockchain
from walytis_beta_api import Blockchain
from mutablockchain import MutaBlockchain


def test_generic_blockchain_features():

    blockchain = Blockchain.create()

    test_generic_blockchain(MutaBlockchain, base_blockchain=blockchain)
    blockchain.delete()


def run_tests():
    test_generic_blockchain_features()


if __name__ == "__main__":
    run_tests()
