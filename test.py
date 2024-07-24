from mutablockchain import MutaBlockchain
from mutablock import MutaBlock
import walytis_beta_api


blockchain: MutaBlockchain
block_id: str
block: MutaBlock


def _on_block_received(block):
    pass


def test_prepare():
    try:
        walytis_beta_api.delete_blockchain("MutablocksTest")
    except:
        pass


def test_create_mutablockchain():
    global blockchain
    print("Creating mutablockchain...")
    blockchain = MutaBlockchain.create(walytis_beta_api.Blockchain, blockchain_name="MutablocksTest",
                                       app_name="tmp", block_received_handler=_on_block_received)


def test_create_mutablock():
    global block
    global blockchain
    print("Loading MutaBlockchain...")

    blockchain = MutaBlockchain(walytis_beta_api.Blockchain, blockchain_id=blockchain.blockchain_id,
                                app_name="tmp", block_received_handler=_on_block_received)
    content = {"message": "hello there", "author": "me"}
    print("Creating mutablock...")
    block = blockchain.add_mutablock(content)
    print("Created mutablock.")
    assert blockchain.get_mutablock(block.id).current_content(
    ) == block.current_content() == content, "Mutablock creation failed"


def test_update_mutablock():
    print("Updating mutablock...")
    updated_content = {"message": "Hello there!", "author": "me"}
    block.edit({"message": "Hello there!"})
    print("Updated mutablock, checking...")
    assert blockchain.get_mutablock(
        block.id).current_content() == block.current_content() == updated_content, "Mutablock update failed"


def test_delete_mutablock():
    print("Deleting mutablock...")
    block.delete()
    # assert blockchain.get_mutablock_ids() == []


def test_delete_mutablockchain():
    print("Deleting mutablockchain...")
    blockchain.delete()
    assert blockchain.get_mutablock(
        block.id).current_content() == block.current_content() == None, "Mutablock update failed"


def test_cleanup():
    print("Cleaning up...")
    blockchain.terminate()


def run_tests():
    test_prepare()
    test_create_mutablockchain()
    test_create_mutablock()
    test_update_mutablock()
    test_delete_mutablock()
    test_delete_mutablockchain()
    test_cleanup()


if __name__ == "__main__":
    run_tests()
