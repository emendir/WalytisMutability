"""A virtual Blockchain with mutable blocks."""
import json
import os
from typing import Callable

import appdirs
import walytis_beta_api
from brenthy_tools_beta.utils import bytes_to_string
from walytis_beta_api import Block, Blockchain

import mutablock
from blockstore import BlockStore
from mutablock import MutaBlock


class MutaBlockchain(BlockStore):
    def __init__(
        self,
        base_blockchain_type,
        blockchain_id: str,
        app_name: str = "",
        block_received_handler: Callable[[Block], None] | None = None,
        auto_load_missed_blocks: bool = True,
        forget_appdata: bool = False,
        sequential_block_handling: bool = True
    ):
        self.db_path = os.path.join(
            appdirs.user_data_dir(),
            "MutaBlockchains",
            blockchain_id
        )
        BlockStore.__init__(self)
        self.init_blockstore()

        self.block_received_handler = block_received_handler
        self.base_blockchain: Blockchain = base_blockchain_type(
            blockchain_id=blockchain_id,
            app_name=app_name,
            block_received_handler=self._on_block_received,
            auto_load_missed_blocks=auto_load_missed_blocks,
            forget_appdata=forget_appdata,
            sequential_block_handling=sequential_block_handling
        )
        self.blockchain_id = self.base_blockchain.blockchain_id

    @classmethod
    def create(
        cls,
        base_blockchain_type,
        blockchain_name: str = "",
        app_name: str = "",
        block_received_handler: Callable[[Block], None] | None = None
    ) -> 'MutaBlockchain':    # pylint: disable=no-self-argument
        blockchain: Blockchain = base_blockchain_type.create(blockchain_name)
        blockchain_id = blockchain.blockchain_id
        blockchain.terminate()

        return cls(
            base_blockchain_type=base_blockchain_type,
            blockchain_id=blockchain_id, app_name=app_name,
            block_received_handler=block_received_handler
        )

    def add_mutablock(self, content: dict, topics: list = []) -> MutaBlock:
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

    def edit_mutablock(self, parent_id: str, content: dict) -> None:
        print("Editing mutablock")
        if isinstance(parent_id, (bytearray, bytes)):
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

    def delete_mutablock(self, parent_id: str) -> None:
        wrapped_content = {
            "type": mutablock.DELETION_BLOCK,
            "parent_block": parent_id,
        }
        block = self.base_blockchain.add_block(


            json.dumps(wrapped_content).encode(),
        )
        self._on_block_received(block)

    def get_mutablock(self, id: str) -> MutaBlock:
        return MutaBlock(id, self)

    def _on_block_received(self, block: walytis_beta_api.Block) -> None:  # pylint: disable=no-self-argument
        print("OBR: Received block!")
        block_id = bytes_to_string(block.short_id)    # pylint: disable=no-member
        print("OBR: Checking known blocks...")
        if block_id in self.get_content_block_ids():
            print("OBR: We already have that block")
            return
        print("OBR: loading block details...")
        block_content = json.loads(
            block.content.decode())  # pylint: disable=no-member

        content_type = block_content['type']
        timestamp = block.creation_time

        print("OBR:", content_type)
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
        print("OBR: Adding mutablock...")
        self.add_content_version(mutablock.ContentVersion(
            type=content_type,
            id=block_id,
            parent_id=parent_id,
            original_id=original_id,
            content=content,
            timestamp=timestamp
        ))
        print("OBR: Finished processing received block.")
        if self.block_received_handler:
            self.block_received_handler(block)

    def delete(self) -> None:
        self.base_blockchain.delete()

    def terminate(self) -> None:
        self.base_blockchain.terminate()

    def __del__(self) -> None:
        self.terminate()
