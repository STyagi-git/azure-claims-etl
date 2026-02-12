from src.utils.hashing import sha256_concat

def test_sha256_concat_stable():
    h1 = sha256_concat(["a", 1, None])
    h2 = sha256_concat(["a", 1, None])
    assert h1 == h2
    assert isinstance(h1, str)
    assert len(h1) == 64
