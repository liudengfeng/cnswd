from cnswd.websource.wy import fetch_history
import pandas as pd


def test_fetch_history_1():
    """测试历史股价"""
    df = fetch_history('000001', '1990-01-01', '2019-11-24')
    assert df.shape == (7041, 15)
    assert df.iat[0, 0] == "'000001"
    assert df.index[0] == pd.Timestamp('2019-11-22 00:00:00')


def test_fetch_history_2():
    """测试历史股价(退市股票)"""
    df = fetch_history('000033', '1990-01-01', '2019-11-24')
    assert df.shape == (5221, 15)
    assert df.iat[0, 0] == "'000033"
    assert df.index[0] == pd.Timestamp('2017-07-06 00:00:00')


def test_fetch_history_3():
    """测试历史股价"""
    df = fetch_history('600000', '1990-01-01', '2019-11-24')
    assert df.shape == (4855, 15)
    assert df.iat[0, 0] == "'600000"
    assert df.index[0] == pd.Timestamp('2019-11-22 00:00:00')
