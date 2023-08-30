import os
import sqlite3
import mutablock
import json
from datetime import datetime
import mutablock

TIME_FORMAT = '%Y.%m.%d_%H.%M.%S.%f'


def time_to_string(time: datetime):
    return time.strftime(TIME_FORMAT)


def string_to_time(string):
    return datetime.strptime(string, TIME_FORMAT)


class BlockStore:
    db_path = "content_versions.db"

    def create_block_database(self):
        db_dir = os.path.dirname(self.db_path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

        # Connect to the SQLite database
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()

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
        connection.commit()

        # Close the connection
        connection.close()

    def add_content_version(self, content_version):
        """Store ContentVersions in the database"""
        print("Storing", content_version.type)
        if content_version.type != mutablock.ORIGINAL_BLOCK:
            original_version = self.verify_original(content_version.parent_id)
            if original_version.id != content_version.original_id:
                raise CorruptContentAncestryError()

        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        cursor.execute("INSERT INTO content_versions (type, id, parent_id, original_id, content, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                       (content_version.type, content_version.id, content_version.parent_id, content_version.original_id,
                        json.dumps(content_version.content), time_to_string(content_version.timestamp)))
        connection.commit()
        connection.close()

    # Retrieve MutaBlock.ContentVersions from the database

    def retrieve_content_versions(self):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()

        cursor.execute(
            "SELECT type, id, parent_id, original_id, content, timestamp FROM content_versions")
        rows = cursor.fetchall()
        content_versions = []
        for row in rows:
            content_version = mutablock.ContentVersion(
                type=row[0], id=row[1], parent_id=row[2], original_id=row[3], content=json.loads(row[4]), timestamp=string_to_time(row[5]))
            content_versions.append(content_version)
        connection.close()
        return content_versions

    # Retrieve a mutablock.ContentVersion from the database by id

    def get_content_version(self, content_version_id: str):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        cursor.execute(
            "SELECT type, parent_id, original_id, content, timestamp FROM content_versions WHERE id = ?", (content_version_id,))
        row = cursor.fetchone()
        connection.close()

        if row:
            return mutablock.ContentVersion(type=row[0], id=content_version_id, parent_id=row[1], original_id=row[2], content=json.loads(row[3]), timestamp=string_to_time(row[4]))
        else:
            return None

    def get_mutablock_content_versions(self, mutablock_id: str):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        cursor.execute(
            '''SELECT type, id, parent_id, original_id, content, timestamp
            FROM content_versions
            WHERE original_id = ?
            ORDER BY timestamp
            ''', (mutablock_id,))
        rows = cursor.fetchall()
        connection.close()

        content_versions = []
        for row in rows:
            content_version = mutablock.ContentVersion(
                type=row[0],
                id=row[1],
                parent_id=row[2],
                original_id=row[3],
                content=json.loads(row[4]),
                timestamp=string_to_time(row[5])
            )
            content_versions.append(content_version)
        return content_versions

    def get_mutablocks(self, ):
        """Returns IDs of MutaBlocks"""
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()

        cursor.execute(
            f'''SELECT id
            FROM content_versions
            WHERE type = ?
            ''', (mutablock.ORIGINAL_BLOCK,))
        rows = cursor.fetchall()
        content_versions = []
        connection.close()
        return [row[0] for row in rows]

    # Delete a mutablock.MutaBlock.ContentVersion from the database based on its id

    def delete_content_version_by_id(self, content_version_id):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        cursor.execute("DELETE FROM content_versions WHERE id = ?",
                       (content_version_id,))
        connection.commit()
        connection.close()

    def verify_original(self, contentv_id):
        """
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


class CorruptContentAncestryError(Exception):
    def __str__(self):
        return "CORRUPT DATA: false original ID found"


def demo():
    create_block_database()
    # Example usage
    cv1 = mutablock.ContentVersion("ORIGINAL_BLOCK", "1", "",
                                   "original1", {"data": "Example 1"})
    cv2 = mutablock.ContentVersion("UPDATE_BLOCK", "2", "1",
                                   "original2", {"data": "Example 2"})

    add_content_version(cv1)
    add_content_version(cv2)

    retrieved_content_versions = retrieve_content_versions()
    for cv in retrieved_content_versions:
        print(cv)

    delete_content_version_by_id("1")
    delete_content_version_by_id("2")


if __name__ == "__main__":
    demo()
