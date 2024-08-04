"""A virtual Blockchain with mutable blocks."""
from typing import Callable

import walytis_beta_api
from brenthy_tools_beta.utils import bytes_to_string, string_to_bytes
from decorate_all import decorate_all_functions
from strict_typing import strictly_typed
from walytis_beta_api import Block, Blockchain
from walytis_beta_api.generic_blockchain import GenericBlock, GenericBlockchain
from .blockstore import BlockStore
from .mutablock import (
    DELETION_BLOCK,
    ORIGINAL_BLOCK,
    UPDATE_BLOCK,
    ContentVersion,
    MutaBlock,
)


class MutaBlockchain(BlockStore, GenericBlockchain):
    block_received_handler: Callable[[GenericBlock], None] | None = None

    def __init__(
        self,
        base_blockchain_type,
        blockchain_id: bytearray | bytes,
        app_name: str = "",
        block_received_handler: Callable[[Block], None] | None = None,
        auto_load_missed_blocks: bool = True,
        forget_appdata: bool = False,
        sequential_block_handling: bool = True
    ):
        # self.db_path = os.path.join(
        #     appdirs.user_data_dir(),
        #     "MutaBlockchains",
        #     blockchain_id
        # )
        BlockStore.__init__(self)
        self.init_blockstore()

        self.block_received_handler = block_received_handler
        self.base_blockchain: Blockchain = base_blockchain_type(
            blockchain_id=blockchain_id,
            app_name=app_name,
            block_received_handler=None,
            auto_load_missed_blocks=False,
            forget_appdata=forget_appdata,
            sequential_block_handling=sequential_block_handling
        )
        self.base_blockchain.block_received_handler = self._on_block_received
        self.base_blockchain.load_missed_blocks(walytis_beta_api.blockchain_model.N_STARTUP_BLOCKS)

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

    def add_block(self, content: bytes | bytearray, topics: list[str] | str = "") -> MutaBlock:
        if isinstance(topics, str):
            topics = []
        topics = [ORIGINAL_BLOCK] + topics
        block = self.base_blockchain.add_block(
            content,
            topics
        )
        self._on_block_received(block)
        print("Created mutablock.")
        return MutaBlock(block, self)

    def edit_block(self, parent_id: bytes | bytearray, content: bytes | bytearray) -> None:
        print("Editing mutablock")

        if isinstance(parent_id, (bytearray, bytes)):
            parent_id = bytes_to_string(parent_id)
        topics = [UPDATE_BLOCK, parent_id]
        block = self.base_blockchain.add_block(
            content,
            topics=topics
        )
        print("Createed update block")
        self._on_block_received(block)

    def delete_block(self, parent_id: bytearray | bytes) -> None:
        if isinstance(parent_id, ContentVersion):
            parent_id = parent_id.cv_id
        topics = [DELETION_BLOCK, bytes_to_string(parent_id)]
        block = self.base_blockchain.add_block(
            content=bytearray([3]),
            topics=topics
        )
        self._on_block_received(block)

    def get_block(self, id: bytearray | bytes) -> MutaBlock:
        return MutaBlock(self.base_blockchain.get_block(id), self)

    def _on_block_received(self, block: walytis_beta_api.Block) -> None:  # pylint: disable=no-self-argument
        # logger.debug("OBR: Received block!")
        block_id = bytes_to_string(
            block.short_id)    # pylint: disable=no-member
        # logger.debug("OBR: Checking known blocks...")
        if block_id in self.get_content_block_ids():
            # logger.debug("OBR: We already have that block")
            return
        # logger.debug("OBR: loading block details...")
        try:
            content_version = self.decode_base_block(block)
        except NotContentVersionBlockError:
            return
        self.add_content_version(content_version)

        # logger.debug("OBR: Finished processing received block.")
        if self.block_received_handler:
            self.block_received_handler(block)

    def decode_base_block(self, block: Block) -> ContentVersion:
        timestamp = block.creation_time

        # logger.debug("OBR:", block.topics[0])
        if len(block.topics) >= 1 and block.topics[0] == ORIGINAL_BLOCK:
            parent_id = bytearray()
            original_id = block.short_id
            user_topics = block.topics[1:]
        elif len(block.topics) >= 2 and block.topics[0] in {UPDATE_BLOCK, DELETION_BLOCK}:
            parent_id = string_to_bytes(block.topics[1])
            original_id = self.verify_original(parent_id).cv_id
            user_topics = block.topics[2:]
        else:
            raise NotContentVersionBlockError()
        # logger.debug("OBR: Adding mutablock...")
        return ContentVersion(
            type=block.topics[0],
            cv_id=block.short_id,
            parent_id=parent_id,
            original_id=original_id,
            content=block.content,
            timestamp=timestamp,
            topics=user_topics,
        )

    def get_peers(self) -> list[str]:
        return self.base_blockchain.get_peers()

    @property
    def app_name(self):
        return self.base_blockchain.app_name

    @property
    def block_ids(self):
        return self.base_blockchain.block_ids

    @property
    def blockchain_id(self):
        return self.base_blockchain.blockchain_id

    @property
    def name(self):
        return self.base_blockchain.name

    def delete(self) -> None:
        self.base_blockchain.delete()

    def terminate(self) -> None:
        self.base_blockchain.terminate()

    def __del__(self) -> None:
        self.terminate()


class NotContentVersionBlockError(Exception):
    pass


decorate_all_functions(strictly_typed, __name__)
