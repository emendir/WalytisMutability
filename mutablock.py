from dataclasses import dataclass
from datetime import datetime

from brenthy_tools_beta.utils import bytes_to_string

import mutablockchain as _mutablockchain

ORIGINAL_BLOCK = "original"
UPDATE_BLOCK = "update"
DELETION_BLOCK = "deletion"


class MutaBlock:
    latest_content_version_id: str

    def __init__(self, id: str, mutablockchain: _mutablockchain.Blockchain):
        if isinstance(id, (bytes, bytearray)):
            id = bytes_to_string(id)

        self.id = id
        self.mutablockchain: _mutablockchain.Blockchain = mutablockchain

    def current_content(self) -> dict:
        """Get the compilation of the multiple ContentVersion's content."""
        content_versions: list[ContentVersion] = self.mutablockchain.get_mutablock_content_versions(
            self.id
        )
        # content_versions are sorted by timestamp,
        # so this way we get the latest one
        latest_content_version = content_versions[-1]
        self.latest_content_version_id = latest_content_version.id
        # in this list we'll chronologically store the contents of this version
        # and all parent version that led to it

        content_history = [latest_content_version.content]
        _version = latest_content_version
        while _version.type not in {ORIGINAL_BLOCK, DELETION_BLOCK}:
            _version = [content_version for content_version in content_versions
                        if content_version.id == _version.parent_id
                        ][0]
            content_history.insert(0, _version.content)
        compiled_content = {}
        for content in content_history:
            print(content)
            # for every json field in this content version
            for key in list(content.keys()):
                # if field already exists in compiled content, update it
                # else add the field as a new field to the compiled_content
                if key in compiled_content:
                    compiled_content[key] = content[key]
                else:
                    compiled_content.update({key: content[key]})

        # create copy of compiled_conttent without fields with None values
        compiled_content_cleaned = {}
        for key in list(compiled_content.keys()):
            if compiled_content[key] is not None:
                compiled_content_cleaned.update({key: compiled_content[key]})

        # if compiled_content_cleaned == {}:
        #     return None
        return compiled_content_cleaned

    def edit(self, content: dict) -> None:
        self.mutablockchain.edit_mutablock(
            self.latest_content_version_id, content)

    def delete(self) -> None:
        self.mutablockchain.delete_mutablock(self.latest_content_version_id)


@dataclass
class ContentVersion:
    type: str
    id: str  # same as the block ID that created this content version
    parent_id: str
    original_id: str
    content: dict
    timestamp: datetime
