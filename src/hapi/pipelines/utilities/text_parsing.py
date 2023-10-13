from typing import Dict

from hdx.location.names import clean_name
from hdx.location.phonetics import Phonetics


def get_code_from_name(
        name: str,
        code_lookup: Dict[str, str],
        code_mapping: Dict[str, str]
) -> str:
    code = code_lookup.get(name)
    if code:
        return code
    name_clean = clean_name(name)
    code = code_mapping.get(name_clean)
    if not code:
        names = list(code_lookup.keys())
        names_lower = [x.lower() for x in names]
        name_index = Phonetics().match(
            possible_names=names_lower,
            name=name,
            alternative_name=name_clean,
        )
        if name_index is None:
            return None
        name = names[name_index]
        code = code_lookup.get(name)
    return code
