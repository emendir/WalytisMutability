"""The machinery for MutaBlock storage in an SQLite database."""
from decorate_all import decorate_all_functions
from strict_typing import strictly_typed
import json
import os
import sqlite3
from abc import ABC, abstractmethod
from brenthy_tools_beta.utils import string_to_time, time_to_string, bytes_to_string
from walytis_beta_api import Block, decode_short_id
from .mutablock import ORIGINAL_BLOCK, ContentVersion, BLOCK_TYPES
from .threaded_object import DedicatedThreadClass, run_on_dedicated_thread
from .utils import logger
TIME_FORMAT = '%Y.%m.%d_%H.%M.%S.%f'


class BlockStore(ABC):
    """MutaBlock storage management in an SQLite database."""

    db_path = "content_versions.db"

    @abstractmethod
    def decode_base_block(self, block: Block) -> ContentVersion:
        pass

    def init_blockstore(self) -> None:
        """Initialise."""
        pass

    def add_content_version(self, content_version: ContentVersion) -> None:
        """Store ContentVersions in the database."""
        pass

    # Retrieve a ContentVersion from the database by id

    def get_content_version(
        self, content_version_id: bytes | bytearray
    ) -> ContentVersion | None:
        """Get a ContentVersion given its ID."""
        return self.decode_base_block(
            self.base_blockchain.get_block(content_version_id)
        )

    def get_mutablock_content_version_ids(
        self, mutablock_id: bytearray | bytes
    ) -> list[bytearray]:
        """Get the content versions of the specified MutaBlock."""
        content_version_ids = [mutablock_id]
        mutablock_id_str = bytes_to_string(mutablock_id)
        for block_id in self.base_blockchain.block_ids:
            topics = decode_short_id(block_id)["topics"]
            if (
                len(topics) >= 2
                and topics[0] in BLOCK_TYPES and topics[1] == mutablock_id_str
            ):
                content_version_ids.append(block_id)
        content_version_ids.sort(
            key=lambda block_id: decode_short_id(block_id)["creation_time"]
        )
        return content_version_ids

    def get_mutablock_content_versions(
        self, mutablock_id: bytearray | bytes
    ) -> list[ContentVersion]:
        return [
            self.decode_base_block(
                self.base_blockchain.get_block(block_id)
            )
            for block_id in self.get_mutablock_content_version_ids(mutablock_id)
        ]

    def get_mutablock_ids(self, ) -> list[str]:
        """Get the IDs of all MutaBlocks."""
        mutablock_ids = []
        for block_id in self.base_blockchain.block_ids:
            topics = decode_short_id(block_id)["topics"]
            if len(topics) >= 1 and topics[0] == ORIGINAL_BLOCK:
                mutablock_ids.append(block_id)
        return mutablock_ids

    def get_content_block_ids(self, ) -> list[str]:
        """Get the IDs of all MutaBlocks."""

        content_version_ids = []
        for block_id in self.base_blockchain.block_ids:
            topics = decode_short_id(block_id)["topics"]
            if len(topics) >= 1 and topics[0] in BLOCK_TYPES:
                content_version_ids.append(block_id)
        return content_version_ids
    # Delete a mutablock.MutaBlock.ContentVersion from the database based on its id

    def verify_original(self, contentv_id: bytearray | bytes) -> ContentVersion:
        """Verify the consistency of a ContentVersion's chain of parents.

        Verifies if the original_id of the chain of parents of a content_version
        are consistent. Raises an exception if not,
        returns the original content_version object if yes.
        """
        # logger.debug(f"Verifying original:\n{contentv_id}")
        parent_version = self.get_content_version(contentv_id)
        expected_original_id = parent_version.original_id
        while parent_version.type != ORIGINAL_BLOCK:
            parent_version = self.get_content_version(parent_version.parent_id)
            if parent_version.original_id != expected_original_id:
                raise CorruptContentAncestryError()
                return None
        return parent_version  # return original content_version

    def terminate(self) -> None:
        self.db.close()
        DedicatedThreadClass.terminate(self)

    def __del__(self) -> None:
        self.terminate()


class CorruptContentAncestryError(Exception):
    def __str__(self):
        return "CORRUPT DATA: false original ID found"


decorate_all_functions(strictly_typed, __name__)
