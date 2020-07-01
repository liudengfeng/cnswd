import pytest
import pandas as pd


@pytest.mark.parametrize("code, start, end", [
    ('000333', '2020-01-01', '2020-05-31'),
])
def test_level_3(fast_api, code, start, end):
    """测试输入日期期间"""
    level = '3'
    data = fast_api.get_data(level, code, start, end)
    assert len(data) == 85


@pytest.mark.parametrize(
    "code, start, end",
    [
        # 测试接受输入年度
        ('000333', '2017-01-01', '2019-12-31'),
    ])
def test_level_5(fast_api, code, start, end):
    """测试单纯年份"""
    level = '分红指标'  # 测试接受项目全称
    data = fast_api.get_data(level, code, start, end)
    dates = sorted(set([d['分红年度'] for d in data]))
    assert len(data) == 6
    assert dates == [
        pd.Timestamp(x) for x in ("2017-06-30", "2017-12-31", "2018-06-30",
                                  "2018-12-31", "2019-06-30", "2019-12-31")
    ]


@pytest.mark.parametrize(
    "code, start, end",
    [
        # 测试接受输入年度
        ('000333', '2017-01-01', '2019-12-31'),
    ])
def test_ttm(fast_api, code, start, end):
    """测试年份、季度"""
    level = '个股TTM现金流量表'
    data = fast_api.get_data(level, code, start, end)
    dates = sorted(set([d['报告年度'] for d in data]))
    assert len(data) == 12
    assert dates == [
        pd.Timestamp(x)
        for x in ("2017-03-31", "2017-06-30", "2017-09-30", "2017-12-31",
                  "2018-03-31", "2018-06-30", "2018-09-30", "2018-12-31",
                  "2019-03-31", "2019-06-30", "2019-09-30", "2019-12-31")
    ]


@pytest.mark.parametrize("code, start, end", [
    ('000333', None, None),
])
def test_level_65(fast_api, code, start, end):
    level = '65'
    data = fast_api.get_data(level, code, start, end)
    assert data[0]['上网发行日期'] == pd.Timestamp('2013-09-17')
