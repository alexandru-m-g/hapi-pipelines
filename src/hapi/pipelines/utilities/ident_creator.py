import hashlib
import random

DEFAULT_SEED = 54321
random.seed(DEFAULT_SEED)


def generate_random_md5() -> str:
    """
    Generate a random MD5 hash using the seed set when the module is imported.

    Returns:
    - md5_hash: A random MD5 hash as a string.
    """

    random_string = str(random.random()).encode("utf-8")
    md5_hash = hashlib.md5(random_string).hexdigest()

    return md5_hash
