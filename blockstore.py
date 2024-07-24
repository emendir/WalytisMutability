"""The machinery for MutaBlock storage in an SQLite database."""
import os
import sqlite3
import mutablock
from mutablock import ContentVersion
import json
from datetime import datetime
from threaded_object import DedicatedThreadClass, run_on_dedicated_thread
from brenthy_tools_beta.utils import string_to_time, time_to_string
TIME_FORMAT = '%Y.%m.%d_%H.%M.%S.%f'


class BlockStore(DedicatedThreadClass):
    """MutaBlock storage management in an SQLite database."""

    db_path = "content_versions.db"

    def __init__(self):
        DedicatedThreadClass.__init__(self)

    @run_on_dedicated_thread
    def init_blockstore(self) -> None:
        """Initialise."""
        db_dir = os.path.dirname(self.db_path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

        # Connect to the SQLite database
        self.db = sqlite3.connect(self.db_path)
        cursor = self.db.cursor()

        # Create a table to store mutablock.MutaBlock.ContentVersions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS content_versions (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                parent_id TEXT NOT NULL,
                original_id TEXT NOT NULL,
                content TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
        ''')
        self.db.commit()
        cursor.close()

    @run_on_dedicated_thread
    def add_content_version(self, content_version: ContentVersion) -> None:
        """Store ContentVersions in the database."""
        print("Storing", content_version.type, content_version.id)
        if content_version.type != mutablock.ORIGINAL_BLOCK:
            original_version = self.verify_original(content_version.parent_id)
            if original_version.id != content_version.original_id:
                raise CorruptContentAncestryError()

        cursor = self.db.cursor()
        cursor.execute(
            '''INSERT INTO content_versions (
                type, id, parent_id, original_id, content, timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                content_version.type,
                content_version.id,
                content_version.parent_id,
                content_version.original_id,
                json.dumps(content_version.content),
                time_to_string(content_version.timestamp)
            )
        )
        self.db.commit()
        cursor.close()

    # Retrieve MutaBlock.ContentVersions from the database

    @run_on_dedicated_thread
    def retrieve_content_versions(self) -> list[ContentVersion]:
        cursor = self.db.cursor()

        cursor.execute(
            "SELECT type, id, parent_id, original_id, content, timestamp FROM content_versions")
        rows = cursor.fetchall()
        content_versions = []
        for row in rows:
            content_version = ContentVersion(
                type=row[0], id=row[1], parent_id=row[2], original_id=row[3], content=json.loads(row[4]), timestamp=string_to_time(row[5]))
            content_versions.append(content_version)
        cursor.close()
        return content_versions

    # Retrieve a ContentVersion from the database by id

    @run_on_dedicated_thread
    def get_content_version(
        self, content_version_id: str
    ) -> ContentVersion | None:
        """Get a ContentVersion given its ID."""
        cursor = self.db.cursor()
        cursor.execute(
            "SELECT type, parent_id, original_id, content, timestamp FROM content_versions WHERE id = ?", (content_version_id,))
        row = cursor.fetchone()
        cursor.close()

        if row:
            return ContentVersion(type=row[0], id=content_version_id, parent_id=row[1], original_id=row[2], content=json.loads(row[3]), timestamp=string_to_time(row[4]))
        else:
            return None

    @run_on_dedicated_thread
    def get_mutablock_content_versions(
        self, mutablock_id: str
    ) -> list[ContentVersion]:
        """Get the content versions of the specified MutaBlock."""
        cursor = self.db.cursor()
        cursor.execute(
            '''SELECT type, id, parent_id, original_id, content, timestamp
            FROM content_versions
            WHERE original_id = ?
            ORDER BY timestamp
            ''', (mutablock_id,))
        rows = cursor.fetchall()
        cursor.close()

        content_versions = []
        for row in rows:
            content_version = ContentVersion(
                type=row[0],
                id=row[1],
                parent_id=row[2],
                original_id=row[3],
                content=json.loads(row[4]),
                timestamp=string_to_time(row[5])
            )
            content_versions.append(content_version)
        return content_versions

    @run_on_dedicated_thread
    def get_mutablock_ids(self, ) -> list[str]:
        """Get the IDs of all MutaBlocks."""
        cursor = self.db.cursor()

        cursor.execute(
            f'''SELECT id
            FROM content_versions
            WHERE type = ?
            ''', (mutablock.ORIGINAL_BLOCK,))
        rows = cursor.fetchall()
        cursor.close()
        return [row[0] for row in rows]

    @run_on_dedicated_thread
    def get_content_block_ids(self, ) -> list[str]:
        """Get the IDs of all MutaBlocks."""
        cursor = self.db.cursor()

        cursor.execute(
            f'''SELECT id
            FROM content_versions
            ''')
        rows = cursor.fetchall()
        cursor.close()
        return [row[0] for row in rows]
    # Delete a mutablock.MutaBlock.ContentVersion from the database based on its id

    @run_on_dedicated_thread
    def delete_content_version_by_id(self, content_version_id: str) -> None:
        cursor = self.db.cursor()
        cursor.execute("DELETE FROM content_versions WHERE id = ?",
                       (content_version_id,))
        self.db.commit()
        cursor.close()

    @run_on_dedicated_thread
    def verify_original(self, contentv_id) -> ContentVersion:
        """Verify the consistency of a ContentVersion's chain of parents.

        Verifies if the original_id of the chain of parents of a content_version
        are consistent. Raises an exception if not,
        returns the original content_version object if yes.
        """
        parent_version = self.get_content_version(contentv_id)
        expected_original_id = parent_version.original_id
        while parent_version.type != mutablock.ORIGINAL_BLOCK:
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


def demo():
    block_store = BlockStore()
    block_store.init_blockstore()
    # Example usage
    cv1 = ContentVersion("ORIGINAL_BLOCK", "1", "",
                         "original1", {"data": "Example 1"})
    cv2 = ContentVersion("UPDATE_BLOCK", "2", "1",
                         "original2", {"data": "Example 2"})

    block_store.add_content_version(cv1)
    block_store.add_content_version(cv2)

    retrieved_content_versions = block_store.retrieve_content_versions()
    for cv in retrieved_content_versions:
        print(cv)

    block_store.delete_content_version_by_id("1")
    block_store.delete_content_version_by_id("2")


if __name__ == "__main__":
    demo()
