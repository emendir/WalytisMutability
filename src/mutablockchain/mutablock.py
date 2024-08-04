from decorate_all import decorate_all_functions
from strict_typing import strictly_typed
from dataclasses import dataclass
from datetime import datetime
from walytis_beta_api.generic_blockchain import GenericBlock
from .mutablockchain import Blockchain
from brenthy_tools_beta.utils import bytes_to_string

ORIGINAL_BLOCK = "MutaBlock-Original"
UPDATE_BLOCK = "MutaBlock-Update"
DELETION_BLOCK = "MutaBlock-Deletion"

BLOCK_TYPES = {ORIGINAL_BLOCK, UPDATE_BLOCK, DELETION_BLOCK}


class MutaBlock(GenericBlock):

    def __init__(self, base_block: GenericBlock, mutablockchain: Blockchain):
        self.mutablockchain: Blockchain = mutablockchain
        self.base_block = base_block

    def get_content_versions(self):
        return self.mutablockchain.get_mutablock_content_versions(self.short_id)

    def get_content_version_ids(self):
        return self.mutablockchain.get_mutablock_content_version_ids(self.short_id)

    def current_content_version(self) -> dict:
        """Get the compilation of the multiple ContentVersion's content."""
        return self.get_content_versions()[-1]

    def edit(self, content: dict) -> None:
        self.mutablockchain.edit_block(
            self.get_content_version_ids()[-1],
            content
        )

    def delete(self) -> None:
        self.mutablockchain.delete_block(self.get_content_version_ids()[-1])

    @property
    def ipfs_cid(self):
        return self.base_block.ipfs_cid

    @property
    def short_id(self):
        return self.base_block.short_id

    @property
    def long_id(self):
        return self.base_block.long_id

    @property
    def creator_id(self):
        return self.base_block.creator_id

    @property
    def creation_time(self):
        return self.base_block.creation_time

    @property
    def topics(self):
        return self.base_block.topics

    @property
    def content(self):
        return self.base_block.content

    @property
    def parents(self):
        return self.base_block.parents

    @property
    def file_data(self):
        return self.base_block.file_data


@dataclass
class ContentVersion:
    type: str
    cv_id: bytearray | bytes  # same as the block ID that created this content version
    parent_id: bytearray | bytes
    original_id: bytearray | bytes
    content: bytearray | bytes
    timestamp: datetime
    topics: list[str]


decorate_all_functions(strictly_typed, __name__)
