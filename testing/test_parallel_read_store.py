"""
使用pytest-xdist测试

设置为8个CPU

数据文件大约20G。

"""
import pytest
from cnswd.store import WyCjmxStore


@pytest.mark.parametrize("code, start, end, expected_rows", [
    ('000001', '2020-04-02', '2020-04-02', 4127),
    ('600000', '2020-04-02', '2020-04-02', 2254),
])
def test_1(code, start, end, expected_rows):
    df = WyCjmxStore.query(None, code, start, end)
    assert len(df) == expected_rows


@pytest.mark.parametrize("code, start, end, expected_rows", [
    ('300001', '2020-04-02', '2020-04-02', 3578),
    ('002024', '2020-04-02', '2020-04-02', 3099),
])
def test_2(code, start, end, expected_rows):
    df = WyCjmxStore.query(None, code, start, end)
    assert len(df) == expected_rows
