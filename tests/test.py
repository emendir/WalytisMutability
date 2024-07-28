
import os

import _testing_utils
import mutablockchain
import walytis_beta_api as waly
from _testing_utils import mark, test_threads_cleanup
from mutablockchain import MutaBlock, MutaBlockchain

_testing_utils.assert_is_loaded_from_source(
    source_dir=os.path.dirname(os.path.dirname(__file__)), module=mutablockchain
)

blockchain: MutaBlockchain
block_id: str
block: MutaBlock


def _on_block_received(block):
    pass


def test_prepare():
    if "MutablocksTest" in waly.list_blockchain_names():
        waly.delete_blockchain("MutablocksTest")


def test_create_mutablockchain():
    global blockchain
    print("Creating mutablockchain...")
    blockchain = MutaBlockchain.create(
        waly.Blockchain,
        blockchain_name="MutablocksTest",
        app_name="tmp",
        block_received_handler=_on_block_received
    )
    mark(
        blockchain.blockchain_id in waly.list_blockchain_ids(),
        "Create Mutablockchain"
    )


def test_create_mutablock():
    global block
    global blockchain
    print("Loading MutaBlockchain...")

    blockchain = MutaBlockchain(waly.Blockchain, blockchain_id=blockchain.blockchain_id,
                                app_name="tmp", block_received_handler=_on_block_received)
    content = {"message": "hello there", "author": "me"}
    print("Creating mutablock...")
    block = blockchain.add_mutablock(content)
    print("Created mutablock.")
    mark(
        blockchain.get_mutablock(block.id).current_content(
        ) == block.current_content() == content,
        "Mutablock creation"
    )


def test_update_mutablock():
    print("Updating mutablock...")
    updated_content = {"message": "Hello there!", "author": "me"}
    block.edit({"message": "Hello there!"})
    print("Updated mutablock, checking...")
    mark(blockchain.get_mutablock(
        block.id).current_content() == block.current_content() == updated_content,
        "Mutablock update"
    )


def test_delete_mutablock():
    print("Deleting mutablock...")
    block.delete()
    # assert blockchain.get_mutablock_ids() == []


def test_delete_mutablockchain():
    print("Deleting mutablockchain...")
    blockchain.delete()
    mark(
        blockchain.blockchain_id not in waly.list_blockchain_ids(),
        "Delete Mutablockchain"
    )


def test_cleanup():
    print("Cleaning up...")
    blockchain.terminate()
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
