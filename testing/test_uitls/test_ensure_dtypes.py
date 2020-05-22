import pytest

from cnswd.utils import ensure_dtypes
import pandas as pd


def simulation_data():
    return pd.DataFrame({
        '字符': ['未知', None, '-', '--', 'ok'],
        '日期': ['2019-01-01', '2019-01-02', None, '-', '2019-01-05'],
        '整数1': [1, 2, 3, None, '-'],
        '整数2': [1, 2.0, 3.0, 4, '-'],
        '浮点1': [1.2, 2.3, '-', 4.4, 5.0],
        '浮点2': ['1.2', '2.3', None, '4.4', '5.0'],
    })


def test_date_ns():
    """测试日期类型"""
    cols = ['日期']
    data = simulation_data()
    actual = ensure_dtypes(data, cols, [], [], [])
    for col in cols:
        assert pd.api.types.is_datetime64_ns_dtype(actual[col])


def test_str():
    """测试字符类型"""
    cols = ['字符']
    data = simulation_data()
    actual = ensure_dtypes(data, [], cols, [], [])
    for col in cols:
        assert pd.api.types.is_object_dtype(actual[col])


def test_int64():
    """测试整数类型"""
    cols = ['整数1', '整数2']
    data = simulation_data()
    actual = ensure_dtypes(data, [], [], cols, [])
    for col in cols:
        assert pd.api.types.is_int64_dtype(actual[col])


def test_float64():
    """测试浮点类型"""
    cols = ['浮点1', '浮点2']
    data = simulation_data()
    actual = ensure_dtypes(data, [], [], [], cols)
    for col in cols:
        assert pd.api.types.is_float_dtype(actual[col])
