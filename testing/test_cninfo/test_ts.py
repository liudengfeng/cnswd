import pytest
import pandas as pd


@pytest.mark.parametrize("level, start, end", [
    ('82', '2020-05-28', '2020-06-02'),
])
def test_fetch_data(ts_api, level, start, end):
    num = 1688
    t_days = 4
    data = ts_api.get_data(level, start, end)
    assert len(data) == num * t_days
    df = pd.DataFrame.from_dict(data)
    assert df.shape == (num * t_days, 9)
    assert len(df['交易日期'].unique()) == t_days