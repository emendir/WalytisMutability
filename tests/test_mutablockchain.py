
from strict_typing import strictly_typed
from decorate_all import decorate_all_functions
import os

import _testing_utils
import mutablockchain
import walytis_beta_api as waly
from _testing_utils import mark, test_threads_cleanup
from mutablockchain import MutaBlock, MutaBlockchain
from walytis_beta_api import Blockchain

_testing_utils.assert_is_loaded_from_source(
    source_dir=os.path.dirname(os.path.dirname(__file__)), module=mutablockchain
)

m_blockchain: MutaBlockchain
block_id: bytearray | bytes
block: MutaBlock


def _on_block_received(block):
    pass


def test_prepare():
    if "MutablocksTest" in waly.list_blockchain_names():
        print("Deleting mutablockchain...")
        waly.delete_blockchain("MutablocksTest")


def test_create_mutablockchain():
    global m_blockchain
    global base_blockchain
    print("Creating mutablockchain...")
    base_blockchain = Blockchain.create()
    m_blockchain = MutaBlockchain(base_blockchain)
    mark(
        m_blockchain.blockchain_id in waly.list_blockchain_ids(),
        "Create Mutablockchain"
    )


def test_create_mutablock():
    global block
    global m_blockchain
    print("Loading MutaBlockchain...")

    m_blockchain = MutaBlockchain(
        base_blockchain=base_blockchain,
        block_received_handler=_on_block_received
    )
    content = "Hello world!".encode()
    print("Creating mutablock...")
    block = m_blockchain.add_block(content)
    print("Created mutablock.")
    mark(
        m_blockchain.get_block(block.short_id).current_content_version(
        ).content == block.current_content_version().content == content,
        "Mutablock creation"
    )


def test_update_mutablock():
    print("Updating mutablock...")
    updated_content = "Hello there!".encode()
    block.edit(updated_content)
    print("Updated mutablock, checking...")
    mark(m_blockchain.get_block(
        block.short_id).current_content_version().content == block.current_content_version().content == updated_content,
        "Mutablock update"
    )


def test_delete_mutablock():
    print("Deleting mutablock...")
    block.delete()
    # assert m_blockchain.get_mutablock_ids() == []


def test_delete_mutablockchain():
    print("Deleting mutablockchain...")
    m_blockchain.delete()
    mark(
        m_blockchain.blockchain_id not in waly.list_blockchain_ids(),
        "Delete Mutablockchain"
    )


def test_cleanup():
    print("Cleaning up...")
    m_blockchain.terminate()
    test_threads_cleanup()


def run_tests():
    test_prepare()
    test_create_mutablockchain()
    test_create_mutablock()
    test_update_mutablock()
    test_delete_mutablock()
    test_delete_mutablockchain()
    test_cleanup()


if __name__ == "__main__":
    _testing_utils.PYTEST = False
    run_tests()

decorate_all_functions(strictly_typed, __name__)
