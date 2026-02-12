import hashlib
from typing import Iterable

def sha256_concat(values: Iterable[str]) -> str:
    """Stable row hash for idempotent merges."""
    m = hashlib.sha256()
    for v in values:
        m.update((str(v) if v is not None else "").encode("utf-8"))
        m.update(b"|")
    return m.hexdigest()
