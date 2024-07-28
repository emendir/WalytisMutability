from decorate_all import decorate_all_functions
from strict_typing import strictly_typed
from dataclasses import dataclass
from datetime import datetime

from .mutablockchain import Blockchain
from brenthy_tools_beta.utils import bytes_to_string

ORIGINAL_BLOCK = "MutaBlock-Original"
UPDATE_BLOCK = "MutaBlock-Update"
DELETION_BLOCK = "MutaBlock-Deletion"

BLOCK_TYPES = {ORIGINAL_BLOCK, UPDATE_BLOCK, DELETION_BLOCK}


class MutaBlock:

    def __init__(self, id: bytes | bytearray, mutablockchain: Blockchain):

        self.id = id
        self.mutablockchain: Blockchain = mutablockchain

    def get_content_versions(self):
        return self.mutablockchain.get_mutablock_content_versions(self.id)

    def get_content_version_ids(self):
        return self.mutablockchain.get_mutablock_content_version_ids(self.id)

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


@dataclass
class ContentVersion:
    type: str
    id: bytearray | bytes  # same as the block ID that created this content version
    parent_id: bytearray | bytes
    original_id: bytearray | bytes
    content: bytearray | bytes
    timestamp: datetime
    topics: list[str]


decorate_all_functions(strictly_typed, __name__)
