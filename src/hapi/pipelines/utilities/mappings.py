from typing import Dict

from hdx.location.names import clean_name
from hdx.location.phonetics import Phonetics
from hdx.utilities.text import multiple_replace

MATCH_THRESHOLD = 5


def get_code_from_name(
    name: str, code_lookup: Dict[str, str], code_mapping: Dict[str, str]
) -> (str | None, str, bool):
    """
    Given a name (org type, sector, etc), return the corresponding code.

    Args:
        name (str): Name to match
        code_lookup (dict): Dictionary of official names and codes
        code_mapping (dict): Additional dictionary of unofficial mappings provided by user

    Returns:
        str or None: matching code
        str: clean name
        bool: whether to add the mapping to the unofficial mappings dictionary
    """
    code = code_lookup.get(name)
    if code:
        return code, name, False
    name_clean = clean_name(name)
    name_clean = multiple_replace(
        name_clean, {"_": " ", "-": " ", ",": "", ".": "", ":": ""}
    )
    name_clean = multiple_replace(name_clean, {"   ": " ", "  ": " "})
    code = code_mapping.get(name_clean)
    if code:
        return code, name_clean, False
    if len(name) <= MATCH_THRESHOLD:
        return None, name_clean, False
    names = list(code_lookup.keys())
    names_lower = [x.lower() for x in names]
    name_index = Phonetics().match(
        possible_names=names_lower,
        name=name,
        alternative_name=name_clean,
    )
    if name_index is None:
        return None, name_clean, False
    name = names[name_index]
    code = code_lookup.get(name, code_mapping.get(name))
    return code, name_clean, True
