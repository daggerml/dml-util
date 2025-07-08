"""Tests for utility functions in dml_util.core.utils."""

from functools import reduce

from dml_util.core.utils import dict_product


class TestDictProduct:
    """Tests for the dict_product utility function."""

    def test_dict_product(self):
        """Test dictionary product generation."""
        param_dict = {"foo": [2, 3], "bar": "abc", "baz": [[5, 5], [5, 8]]}
        piter = list(dict_product(param_dict))
        total_len = reduce(lambda a, b: a * len(b), param_dict.values(), 1)
        assert len(piter) == total_len
        for k, v in param_dict.items():
            assert len([x for x in piter if x[k] == v[0]]) == total_len / len(v)

