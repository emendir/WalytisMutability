import sqlite3
import mutablock
import json
from datetime import datetime

DB_PATH = "content_versions.db"
TIME_FORMAT = '%Y.%m.%d_%H.%M.%S.%f'


def time_to_string(time: datetime):
    return time.strftime(TIME_FORMAT)


def string_to_time(string):
    return datetime.strptime(string, TIME_FORMAT)


def create_if_not_exists():
    # Connect to the SQLite database
    connection = sqlite3.connect(DB_PATH)
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


def store_content_version(content_version):
    """Store ContentVersions in the database"""
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.execute("INSERT INTO content_versions (type, id, parent_id, original_id, content, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                   (content_version.type, content_version.id, content_version.parent_id, content_version.original_id,
                    json.dumps(content_version.content), time_to_string(content_version.timestamp)))
    connection.commit()
    connection.close()

# Retrieve MutaBlock.ContentVersions from the database


def retrieve_content_versions():
    connection = sqlite3.connect(DB_PATH)
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


def retrieve_content_version_by_id(content_version_id: str):
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.execute(
        "SELECT type, parent_id, original_id, content, timestamp FROM content_versions WHERE id = ?", (content_version_id,))
    row = cursor.fetchone()
    connection.close()

    if row:
        return mutablock.ContentVersion(type=row[0], id=content_version_id, parent_id=row[1], original_id=row[2], content=json.loads(row[3]), timestamp=string_to_time(row[4]))
    else:
        return None


def retrieve_mutablock_content_versions(mutablock_id: str):
    connection = sqlite3.connect(DB_PATH)
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


def retrieve_content_version_originals():
    """Returns IDs of MutaBlocks"""
    connection = sqlite3.connect(DB_PATH)
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


def delete_content_version_by_id(content_version_id):
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM content_versions WHERE id = ?",
                   (content_version_id,))
    connection.commit()
    connection.close()


def demo():
    # Example usage
    cv1 = mutablock.ContentVersion("ORIGINAL_BLOCK", "1", "",
                                   "original1", {"data": "Example 1"})
    cv2 = mutablock.ContentVersion("UPDATE_BLOCK", "2", "1",
                                   "original2", {"data": "Example 2"})

    store_content_version(cv1)
    store_content_version(cv2)

    retrieved_content_versions = retrieve_content_versions()
    for cv in retrieved_content_versions:
        print(cv)

    delete_content_version_by_id("1")
    delete_content_version_by_id("2")


create_if_not_exists()
if __name__ == "__main__":
    demo()
