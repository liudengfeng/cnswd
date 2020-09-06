from cnswd.utils.path_utils import data_root
import pandas as pd
from cnswd.websource.treasuries import fetch_treasury_data_from


DATA_DIR = data_root('treasury')  # 在该目录存储国债利率数据


def test_full_years(end_year=2019):
    """测试年份是否完整"""
    actual = []
    for p in sorted(DATA_DIR.glob('*.xlsx')):
        y = int(p.name[:4])
        if y <= end_year:
            actual.append(y)
    expected = [y for y in range(2002, end_year+1)]
    assert actual == expected


def test_fetched_data(end_date='2019-11-24'):
    """测试现有数据是否正确"""
    df = fetch_treasury_data_from(end=end_date)
    assert df.shape == (4446, 16)
    assert df.index[0] == pd.Timestamp('2002-01-04 00:00:00')
    assert df.index[-1] == pd.Timestamp('2019-11-22 00:00:00')
