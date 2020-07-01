"""
国库券收益率模块

1. 将资源目录下的数据拷贝至`DATA_DIR`
2. 下载当年的数据
3. 检查数据年度是否完整

每个交易日后，用后台任务完成`download_last_year`
"""
from __future__ import absolute_import, division, print_function

import datetime
import os
import re
import time
from os.path import expanduser
from shutil import move
from pathlib import Path
import pandas as pd

from ..utils import data_root, sanitize_dates
from .._seleniumwire import make_headless_browser_with_auto_save_path

EARLIEST_POSSIBLE_DATE = pd.Timestamp('2002-1-4', tz='UTC')

DB_COLS_NAME = [
    'm0', 'm1', 'm2', 'm3', 'm6', 'm9', 'y1', 'y3', 'y5', 'y7', 'y10', 'y15',
    'y20', 'y30', 'y40', 'y50'
]
DB_INDEX_NAME = 'date'

OUTPUT_COLS_NAME = [
    '0month', '1month', '2month', '3month', '6month', '9month', '1year',
    '3year', '5year', '7year', '10year', '15year', '20year', '30year',
    '40year', '50year'
]
OUTPUT_INDEX_NAME = 'Time Period'
DATA_DIR = data_root('treasury')  # 在该目录存储国债利率数据


def read_local_data():
    """读取本地文件数据"""
    dfs = []
    for fp in DATA_DIR.glob('*.xlsx'):
        df = pd.read_excel(fp, index_col='日期', parse_dates=True)
        dfs.append(df)
    return pd.concat(dfs)


def download_last_year():
    """下载当年国债利率数据至本地"""
    download_path = os.path.join(expanduser('~'), 'Downloads')
    content_type = 'application/x-msdownload'
    url = 'http://yield.chinabond.com.cn/cbweb-mn/yield_main?locale=zh_CN#'
    driver = make_headless_browser_with_auto_save_path(download_path,
                                                       content_type)
    driver.get(url)
    time.sleep(1)
    # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    css = '.t1 > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(4) > a:nth-child(1)'
    driver.find_element_by_css_selector(css).click()
    driver.switch_to.window(driver.window_handles[-1])
    time.sleep(1)
    try:
        d_btn_css = 'body > form:nth-child(1) > table:nth-child(2) > tbody:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > a:nth-child(1) > h4:nth-child(1)'
        driver.find_element_by_css_selector(d_btn_css).click()
        # 等待下载完成后才关闭浏览器
        time.sleep(1)
    except Exception as e:
        print(f'{e!r}')
    for root, _, files in os.walk(download_path):
        for name in files:
            if name.endswith("xlsx") and name.find('中债国债收益率曲线标准期限信息'):
                src = os.path.join(root, name)
                dst = os.path.join(DATA_DIR,
                                   f'{datetime.date.today().year}.xlsx')
                move(src, dst)
                break
    driver.quit()


def _preprocess(df, start, end):
    """选取及处理指定期间的数据"""
    df.index = pd.to_datetime(df.index)
    df = df[start:end]
    if df.empty:
        return pd.DataFrame()
    pivot_data = df.pivot(columns='标准期限(年)', values='收益率(%)')
    labels = [
        0, 0.08, 0.17, 0.25, 0.5, 0.75, 1, 3, 5, 7, 10, 15, 20, 30, 40, 50
    ]
    pivot_data = pivot_data.reindex(labels, axis="columns")
    data = pivot_data.loc[:, labels]
    data.columns = DB_COLS_NAME
    data.index = pd.to_datetime(data.index)
    data.index.name = DB_INDEX_NAME
    # 百分比转换为小数
    return data * 0.01


def fetch_treasury_data_from(start=EARLIEST_POSSIBLE_DATE.date(),
                             end=pd.Timestamp('today')):
    """
    获取期间资金成本数据

    Parameters
    ----------
    start : datelike
        开始日期
    end : datelike
        结束日期

    Returns
    -------
    res : DataFrame
        Index: 日期
        columns:月度年度周期

    Example
    -------
    >>> df = fetch_treasury_data_from('2017-11-1','2017-11-20')
    >>> df.columns
    Index(['m0', 'm1', 'm2', 'm3', 'm6', 'm9', 'y1', 'y3', 'y5', 'y7', 'y10','y15', 'y20', 'y30', 'y40', 'y50'],dtype='object')
    >>> df.iloc[:,:6]
                    m0        m1        m2        m3        m6        m9
    date
    2017-11-01  0.030340  0.030800  0.030909  0.035030  0.035121  0.035592
    2017-11-02  0.029894  0.029886  0.032182  0.035074  0.035109  0.035493
    2017-11-03  0.027311  0.030052  0.032532  0.034992  0.035017  0.035461
    2017-11-06  0.026155  0.030086  0.032532  0.034917  0.034992  0.035514
    2017-11-07  0.026155  0.030127  0.032813  0.034788  0.035039  0.035465
    2017-11-08  0.026759  0.029984  0.033226  0.035399  0.035034  0.035469
    2017-11-09  0.027285  0.029925  0.033655  0.035553  0.034849  0.035629
    2017-11-10  0.027618  0.029958  0.033720  0.035691  0.035939  0.035735
    2017-11-13  0.028462  0.030854  0.034653  0.035708  0.035939  0.035935
    2017-11-14  0.028462  0.031018  0.034988  0.035754  0.035939  0.035940
    2017-11-15  0.028384  0.030871  0.035439  0.036412  0.036566  0.036252
    2017-11-16  0.028338  0.030875  0.035427  0.036317  0.036502  0.036222
    2017-11-17  0.027718  0.029956  0.035390  0.036981  0.036752  0.036183
    2017-11-20  0.028198  0.030235  0.035431  0.036797  0.036686  0.036153
    """
    start, end = sanitize_dates(start, end)
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    df = read_local_data()
    return _preprocess(df, start, end)
