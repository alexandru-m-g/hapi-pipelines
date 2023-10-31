from typing import Dict

from hdx.location.names import clean_name
from hdx.location.phonetics import Phonetics


def get_code_from_name(
    name: str, code_lookup: Dict[str, str], code_mapping: Dict[str, str]
) -> str | None:
    """
    Given a name (org type, sector, etc), return the corresponding code.

    Args:
        name (str): Name to match
        code_lookup (dict): Dictionary of official names and codes
        code_mapping (dict): Additional dictionary of unofficial mappings provided by user

    Returns:
        str or None: matching code
    """
    code = code_lookup.get(name)
    if code:
        return code
    name_clean = clean_name(name)
    code = code_mapping.get(name_clean)
    if code:
        return code
    names = list(code_lookup.keys())
    names_lower = [x.lower() for x in names]
    name_index = Phonetics().match(
        possible_names=names_lower,
        name=name,
        alternative_name=name_clean,
    )
    if name_index is None:
        names = list(code_mapping.keys())
        names_lower = [x.lower() for x in names]
        name_index = Phonetics().match(
            possible_names=names_lower,
            name=name,
            alternative_name=name_clean,
        )
    if name_index is None:
        return None
    name = names[name_index]
    code = code_lookup.get(name, code_mapping.get(name))
    return code
