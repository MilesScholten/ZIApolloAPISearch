import pytest
from enrichment.utils import sanitize_domain, flatten

def test_sanitize_domain():
    assert sanitize_domain("https://www.example.com") == "example.com"
    assert sanitize_domain("http://example.com/abc") == "example.com"
    assert sanitize_domain("example.com") == "example.com"
    assert sanitize_domain("") == ""

def test_flatten():
    obj = {"a": {"b": 2}, "c": [1, {"d": 4}]}
    flat = flatten("zi", obj)
    # Ensure keys are flattened and underscored
    assert "zi_a_b" in flat
    assert "zi_c_0" in flat
    assert "zi_c_1_d" in flat
