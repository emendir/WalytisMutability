from time import sleep

import _testing_utils
import test_mutablockchain
import test_generic_blockchain_features

_testing_utils.PYTEST = False

test_mutablockchain.run_tests()
test_generic_blockchain_features.run_tests()
sleep(1)
