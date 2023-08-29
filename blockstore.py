from datastore_file_io import delete_content_version_by_id, store_content_version, retrieve_content_version_by_id, retrieve_content_version_originals, retrieve_mutablock_content_versions
import mutablock


def add_mutablock(block_id, content):
    content_version = mutablock.ContentVersion(
        type=mutablock.ORIGINAL_BLOCK,
        id=block_id,
        parent_id="",
        original_id=block_id,
        content=content
    )

    add_content_version(content_version)

    return block_id


def update_mutablock(block_id, parent_id: str, content):
    original_version = verify_original(parent_id)

    content_version = mutablock.ContentVersion(
        type=mutablock.UPDATE_BLOCK,
        id=block_id,
        parent_id=parent_id,
        original_id=original_version.id,
        content=content
    )

    add_content_version(content_version)

    return original_version


def delete_mutablock(block_id, parent_id):
    original_version = verify_original(parent_id)

    content_version = mutablock.ContentVersion(
        type=mutablock.DELETION_BLOCK,
        id=block_id,
        parent_id=parent_id,
        original_id=original_version.id,
        content=None
    )

    add_content_version(content_version)

    return original_version


def get_mutablocks():
    """Returns IDs of Mutablocks"""
    return retrieve_content_version_originals()


def add_content_version(content_version):
    print("Storing", content_version.type)
    if content_version.type != mutablock.ORIGINAL_BLOCK:
        original_version = verify_original(content_version.parent_id)
        if original_version.id != content_version.original_id:
            raise CorruptContentAncestryError()
    return store_content_version(content_version)


def get_content_version(id: str):
    return retrieve_content_version_by_id(id)


def get_mutablock_content_versions(id: str):
    return retrieve_mutablock_content_versions(id)


def verify_original(contentv_id):
    """
    Verifies if the original_id of the chain of parents of a content_version
    are consistent. Raises an exception if not,
    returns the original content_version object if yes.
    """
    parent_version = get_content_version(contentv_id)
    expected_original_id = parent_version.original_id
    while parent_version.type != mutablock.ORIGINAL_BLOCK:
        parent_version = get_content_version(parent_version.parent_id)
        if parent_version.original_id != expected_original_id:
            raise CorruptContentAncestryError()
            return None
    return parent_version  # return original content_version


class CorruptContentAncestryError(Exception):
    def __str__(self):
        return "CORRUPT DATA: false original ID found"
