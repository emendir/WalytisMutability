import walytis_beta_api
import os
import json
import mutablock
from blockstore import BlockStore
from brenthy_tools_beta.utils import bytes_to_string, string_to_bytes
import appdirs


class MutaBlockchain(BlockStore):
    def __init__(self,
                 base_blockchain_type,
                 blockchain_id: str,
                 app_name: str = 'temp',
                 block_received_handler=None,
                 auto_load_missed_blocks: bool = True,
                 forget_appdata=False,
                 sequential_block_handling=True
                 ):
        self.db_path = os.path.join(
            appdirs.user_data_dir(),
            "MutaBlockchains",
            blockchain_id
        )
        self.init_blockstore()

        self.block_received_handler = block_received_handler
        self.base_blockchain: walytis_beta_api.Blockchain = base_blockchain_type(
            blockchain_id=blockchain_id,
            app_name=app_name,
            block_received_handler=self._on_block_received,
            auto_load_missed_blocks=auto_load_missed_blocks,
            forget_appdata=forget_appdata,
            sequential_block_handling=sequential_block_handling
        )
        self.blockchain_id = self.base_blockchain.blockchain_id

    def create(base_blockchain_type, blockchain_name: str = "", app_name: str = "", block_received_handler=None):    # pylint: disable=no-self-argument
        blockchain = base_blockchain_type.create(blockchain_name)
        blockchain_id = blockchain.blockchain_id
        blockchain.terminate()

        return MutaBlockchain(base_blockchain_type=base_blockchain_type, blockchain_id=blockchain_id, app_name=app_name,
                              block_received_handler=block_received_handler)

    def add_mutablock(self, content: dict, topics: list = []):
        wrapped_content = {
            "type": mutablock.ORIGINAL_BLOCK,
            "content": content
        }
        block = self.base_blockchain.add_block(
            json.dumps(wrapped_content).encode(),
            topics
        )
        self._on_block_received(block)
        print("Created mutablock.")
        return mutablock.MutaBlock(block.short_id, self)

    def edit_mutablock(self, parent_id: str, content: dict):
        print("Editing mutablock")
        if isinstance(parent_id, bytes) or isinstance(parent_id, bytearray):
            parent_id = bytes_to_string(parent_id)
        wrapped_content = {
            "type": mutablock.UPDATE_BLOCK,
            "parent_block": parent_id,
            "content": content
        }
        block = self.base_blockchain.add_block(
            json.dumps(wrapped_content).encode(),
        )
        print("Createed update block")
        self._on_block_received(block)

    def delete_mutablock(self, parent_id: str):
        wrapped_content = {
            "type": mutablock.DELETION_BLOCK,
            "parent_block": parent_id,
        }
        block = self.base_blockchain.add_block(


            json.dumps(wrapped_content).encode(),
        )
        self._on_block_received(block)

    def get_mutablock_ids(self):
        return self.get_mutablocks()

    def get_mutablock(self, id):
        return mutablock.MutaBlock(id, self)

    def _on_block_received(self, block: walytis_beta_api.Block):  # pylint: disable=no-self-argument
        id = bytes_to_string(block.short_id)    # pylint: disable=no-member
        block_content = json.loads(
            block.content.decode())  # pylint: disable=no-member

        content_type = block_content['type']
        timestamp = block.creation_time

        if content_type == mutablock.ORIGINAL_BLOCK:
            parent_id = ""
            original_id = bytes_to_string(block.short_id)
            content = block_content['content']
        elif content_type == mutablock.UPDATE_BLOCK:
            parent_id = block_content['parent_block']
            original_id = self.verify_original(parent_id).id
            content = block_content['content']
        elif content_type == mutablock.DELETION_BLOCK:
            parent_id = block_content['parent_block']
            original_id = self.verify_original(parent_id).id
            content = {}
        self.add_content_version(mutablock.ContentVersion(
            type=content_type,
            id=id,
            parent_id=parent_id,
            original_id=original_id,
            content=content,
            timestamp=timestamp
        ))

        if self.block_received_handler:
            self.block_received_handler(block)

    def delete(self):
        self.base_blockchain.delete()

    def terminate(self):
        self.base_blockchain.terminate()

    def __del__(self):
        self.terminate()
