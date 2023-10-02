import regex as re

ILLEGAL_CHARACTERS_RE = r"[\000-\010]|[\013-\014]|[\016-\037]"
INVALID_TITLE_REGEX = r"[\\*?:/\[\]]"
