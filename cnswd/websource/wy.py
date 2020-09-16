"""
网易数据模块

数据类别：
    指数代码名称           get_index_base
    主要指数列表           get_main_index
    股票指数交易数据       fetch_history
    股票指数OHLCV数据      fetch_ohlcv
    财务指标               fetch_financial_indicator
    财务报表               fetch_financial_report 
    业绩预告               fetch_performance_notice
    股东变动               fetch_jjcg, fetch_top10_stockholder
    融资融券               fetch_margin_data

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from urllib.error import HTTPError
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
from functools import partial, lru_cache
from io import StringIO, BytesIO

from ..utils import sanitize_dates

from .base import get_page_response, friendly_download
from .._exceptions import NoWebData

_WY_STOCK_HISTORY_NAMES = [
    'name', 'close', 'high', 'low', 'open', 'prev_close', 'change',
    'change_pct', 'turnover', 'volume', 'amount', 'tmv', 'cmv'
]
_OHLCV = ['开盘价', '最高价', '最低价', '收盘价', '成交量']
_WY_INDEX_HISTORY_NAMES = [
    'name', 'close', 'high', 'low', 'open', 'prev_close', 'change',
    'change_pct', 'volume', 'amount'
]
_WY_INDEX_HISTORY_USE_COLS = list(
    set(range(15)).difference((1, 10, 13, 14, 15)))
_WY_STOCK_HISTORY_USE_COLS = list(set(range(15)).difference([1]))
_CJMX_COLS = ('时间', '价格', '涨跌额', '成交量', '成交额', '方向')

_WY_MARGIN_DATA_USE_COLS = [1, 4, 5, 6, 7, 8, 9, 10, 11]
_WY_MARGIN_DATA_COL_NAMES = [
    '股票代码', '融资余额', '融资买入额', '融资偿还额', '融券余量', '融券卖出量', '融券偿还量', '融券余量金额', ''
    '融券余额'
]
_WY_FH_COLS = [
    '公告日期', '分红年度', '送股(每10股)', '转增(每10股)', '派息(每10股)',
    '股权登记日', '除权除息日', '红股上市日'
]

MARGIN_START = pd.Timestamp('2010-3-31').date()


def get_index_base():
    """获取上海及深圳指数代码、名称表"""
    url_fmt = 'http://quotes.money.163.com/hs/service/hsindexrank.php?host=/hs/service/'
    url_fmt += 'hsindexrank.php&page={page}&query=IS_INDEX:true;EXCHANGE:CNSE{ex}&fields=no,SYMBOL,NAME&'
    url_fmt += 'sort=SYMBOL&order=asc&count={count}&type=query'
    one_big_int = 10000  # 设定一个比较大的整数

    def get_index_from(ex):
        url = url_fmt.format_map({'page': 0, 'ex': ex, 'count': one_big_int})
        #response = get_response(url, 'get', 'json')
        response = get_page_response(url, method='post')
        df = pd.DataFrame(response.json()['list'])
        return df.loc[:, ['SYMBOL', 'NAME']]

    # 查询代码（深圳+1，上海+0）
    dfs = [get_index_from('SH'), get_index_from('SZ')]
    df = pd.concat(dfs)
    df.columns = df.columns.str.lower()
    df.rename(columns={'symbol': 'code'}, inplace=True)
    df.set_index('code', inplace=True, drop=True)
    return df


def get_main_index():
    """主要指数列表"""
    df = pd.DataFrame({
        'name': [
            '上证指数', 'A股指数', 'B股指数', '上证50', '沪深300', '深证成指', '深成指R', '成份B指',
            '创业板指', '创业板综', '深证综指'
        ],
        'code': [
            '000001', '000002', '000003', '000016', '000300', '399001',
            '399002', '399003', '399006', '399102', '399106'
        ]
    })
    df.set_index('code', inplace=True)
    return df


def _query_code(code, is_index):
    if not is_index:
        if code[0] == '6':
            code = '0{}'.format(code)
        else:
            code = '1{}'.format(code)
    else:
        if code[0] == '0':
            code = '0{}'.format(code)
        else:
            code = '1{}'.format(code)
    return code


def fetch_history(code, start, end=None, is_index=False):
    """获取股票或者指数的历史交易数据（不复权）
    备注：
        提供的数据延迟一日

    记录：
        `2018-12-12 16：00`时下载 002622 历史数据，数据截至日为2018-12-10 延迟2日
    """
    start, end = sanitize_dates(start, end)
    url_fmt = 'http://quotes.money.163.com/service/chddata.html?code={}&start={}&end={}'
    code = _query_code(code, is_index)
    start_str = start.strftime('%Y%m%d')
    end_str = end.strftime('%Y%m%d')
    url = url_fmt.format(code, start_str, end_str) + '#01b07'
    na_values = ['None', '--', 'none']
    kwds = {
        'index_col': 0,
        'encoding': 'cp936',
        'parse_dates': True,
        'na_values': na_values,
    }
    page_response = get_page_response(url, 'get')
    df = pd.read_csv(BytesIO(page_response.content), **kwds)
    return df


def fetch_ohlcv(code, start, end, is_index=False):
    """提取股票或指数_OHLCV数据（不复权）"""
    return fetch_history(code, start, end, is_index)[_OHLCV]


def fetch_last_history():
    """
    最新日线成交

    注意：
        区别于fetch_history，用于提取全部股票最新的日线交易数据。
    """
    url = "http://quotes.money.163.com/hs/service/diyrank.php?"
    # url += "host=http://quotes.money.163.com/hs/service/diyrank.php&"
    url += "page=0&query=STYPE:EQA&fields=SYMBOL,NAME,PRICE,PERCENT,OPEN,YESTCLOSE,"
    url += "HIGH,LOW,VOLUME,TURNOVER,PE,MCAP,TCAP&sort=PERCENT&"
    url += "order=desc&count=5000&type=query"
    r = requests.get(url)
    df = pd.DataFrame.from_records(r.json()['list'])
    return df


@friendly_download(10, None, 1)
def fetch_cjmx(code, tdate):
    """
    提取股票历史交易明细

    缺点：
        只能下载近期的数据
        当前滞后2日
    """
    tdate = pd.Timestamp(tdate)
    url_fmt = 'http://quotes.money.163.com/cjmx/{qyear}/{qdate}/{qcode}.xls'
    qyear = tdate.year
    qdate = tdate.strftime(r'%Y%m%d')
    qcode = _query_code(code, False)
    url = url_fmt.format_map({'qyear': qyear, 'qdate': qdate, 'qcode': qcode})
    na_values = ['None', '--', 'none']
    kwds = {'na_values': na_values}
    try:
        df = pd.read_excel(url, **kwds)
    except HTTPError:
        raise NoWebData('不存在网页数据。股票：{}，日期：{}'.format(code, tdate.date()))
    df.columns = _CJMX_COLS
    df.insert(0, '日期', tdate)
    df.insert(0, '股票代码', code)
    return df


def fetch_fhpg(code):
    """股票分红配股数据

    返回：
        list
        0： 分红配股
        1： 配股一览
        2： 增发一览
        3： 历年融资计划
    """
    url = f'http://quotes.money.163.com/f10/fhpg_{code}.html#01d05'
    r = requests.get(url)
    attrs = {'class': 'table_bg001 border_box limit_sale'}
    dfs = pd.read_html(r.text, attrs=attrs, na_values=['--', '暂无数据'])
    dfs[0].columns = _WY_FH_COLS
    return dfs


def _cwzb_url(code, type, part):
    """财务指标网址
    """
    url_fmt1 = 'http://quotes.money.163.com/service/zycwzb_{}.html?type={}'
    url_fmt2 = url_fmt1 + '&part={}'
    if part == 'zhzb':
        url = url_fmt1.format(code, type)
    else:
        url = url_fmt2.format(code, type, part)
    return url


def _report_url(code, type_, report):
    """财务报告网址"""
    url_fmt = 'http://quotes.money.163.com/service/{}_{}.html?type={}'
    return url_fmt.format(report, code, type_)


def _parse_report_data(url):
    response = get_page_response(url)
    #response.encoding = 'gb2312'
    # 000001资产负债表 -> 应收出口退税(万元) -> ' --' 导致解析类型不正确！！！
    na_values = ['--', ' --', '-- ']
    return pd.read_csv(StringIO(response.text),
                       na_values=na_values).iloc[:, :-1]


@friendly_download()
def fetch_financial_indicator(code, report_type, part):
    """
    财务指标
    ---------------
        类别：
            report_type      含义
            ———————————      ———————
            report           按报告期
            year             按年度
            season           按单季度
        项目：
            part             含义
            ————————         ——————
            zhzb             综合指标
            ylnl             盈利能力
            chnl             偿还能力
            cznl             成长能力
            yynl             营运能力
    """
    assert report_type in ('report', 'year', 'season')
    assert part in ('zhzb', 'ylnl', 'chnl', 'cznl', 'yynl')
    url = _cwzb_url(code, report_type, part)
    data = pd.read_csv(url, na_values=['--', ' --', '-- '],
                       encoding='gb2312').iloc[:, :-1]
    return data


@friendly_download()
def fetch_financial_report(code, report_type, report_item):
    """
    财务报表
    ---------------
        类别：
            report_type     含义
            ———————————     ———————
            report          按报告期
            year            按年度

        项目：
            report_item     含义
            ———————————     ———————
            lrb             利润表
            zcfzb           资产负债表
            xjllb           现金流量表
    """
    assert report_type in ('report', 'year')
    assert report_item in ('lrb', 'zcfzb', 'xjllb')
    url = _report_url(code, report_type, report_item)
    data = pd.read_csv(url, na_values=['--', ' --', '-- '],
                       encoding='gb18030').iloc[:, :-1]
    return data


def _parse_performance_notice(raw_df):
    """将DataFrame对象解析为字典"""
    data = {}
    data['date'] = pd.to_datetime(raw_df.iloc[0, 1], errors='coerce')
    data['announcement_date'] = pd.to_datetime(raw_df.iloc[0, 3],
                                               errors='coerce')
    data['notice_type'] = raw_df.iloc[1, 1]
    data['forecast_summary'] = raw_df.iloc[2, 1]
    data['forecast_content'] = raw_df.iloc[3, 1]
    return data


def fetch_performance_notice(stock_code, output_type='list'):
    """业绩预告
    如果输出类型为list，则输出dict列表
    否则，输出DataFrame对象。
    """
    url_fmt = 'http://quotes.money.163.com/f10/yjyg_{}.html'
    url = url_fmt.format(stock_code)
    response = get_page_response(url)
    dfs = pd.read_html(response.text,
                       match='报告日期',
                       attrs={'class': 'table_bg001 border_box table_details'})
    result = [_parse_performance_notice(df) for df in dfs]
    if output_type == 'list':
        return result
    else:
        df = pd.DataFrame.from_dict(result)
        df.set_index('announcement_date', inplace=True)
        return df


def fetch_company_info(stock_code):
    """获取公司简介、IPO信息（作为巨潮公司简要信息的补充）"""
    url_fmt = 'http://quotes.money.163.com/f10/gszl_{}.html'
    url = url_fmt.format(stock_code)
    attrs = {'class': 'table_bg001 border_box limit_sale table_details'}
    res = pd.read_html(url, attrs=attrs)
    return res[0], res[1]


@lru_cache(None)
def fetch_report_periods(stock_code, query):
    """
    下载股东持股、基金持股时，网络已有可供下载的期间

    返回：dict对象
        键：期末日期 eg 2017-06-30
        值：2017-06-30,2017-03-31
    """
    valid_types = ('c', 't', 'jjcg')
    assert query in valid_types, '{}不在有效类型{}中'.format(query, valid_types)
    if query == 'jjcg':
        type_ = 'jjcg'
        target_num = 0
    elif query == 't':
        type_ = 'gdfx'
        target_num = 1
    else:
        type_ = 'gdfx'
        target_num = 0
    result = {}
    url_fmt = 'http://quotes.money.163.com/f10/{type}_{stock_code}.html'
    url = url_fmt.format(stock_code=stock_code, type=type_)
    response = get_page_response(url, 'post')
    soup = BeautifulSoup(response.text, 'lxml')
    ss = soup.find_all('select', {'id': '', 'name': ''})
    # 找到对应的选项父节点
    target = ss[target_num]
    for o in target.find_all('option'):
        if len(o['value']):
            result[o.string] = o['value']
    return result


@friendly_download()
def fetch_top10_stockholder(stock_code, query_date, type_='c'):
    """
    给定股票代码、期末日期、数据类型，返回股东数据

    Parameters
    ----------
    stock_code : str
      股票代码（6位）

    query_date : date_like
      要查询的期末日期

    type_ : str
      查询类别代码：
        c : 前10大流通股东
        t : 前10大股东（含非流通股权股东）

    Returns
    -------
    res : DataFrame
        包含4列，如案例所示

    Notes
    -----
        如果查询日期不在可选期间，会报错

    Example
    -------
    >>> fetch_top10_stockholder('000001','2004-09-30')
                    十大流通股东   持有比例  本期持有股(万股) 持股变动数(万股)
    0             普丰证券投资基金  0.68%    1315.39   减持77.50
    1                国债服务部  0.38%     734.07        不变
    2           博时裕富证券投资基金  0.34%     657.45   增持44.67
    3            深圳市投资管理公司  0.32%     613.25        不变
    4                  张绍红  0.31%     598.75   减持47.85
    5      融通深证100指数证券投资基金  0.25%     477.90   增持87.12
    6                  孟常春  0.24%     463.38   减持45.92
    7                  王秋生  0.15%     300.12   增持15.46
    8  长城久泰中信标普300指数证券投资基金  0.15%     298.46        新进
    9                  靳艳敏  0.11%     210.64   减持19.46
    >>> # 以前，并不要求报告一季度及三季度数据
    >>> fetch_top10_stockholder('000001','1993-06-30', 't')
              十大股东   持有比例  本期持有股(万股) 持股变动数(万股)
    0    深圳市投资管理公司  8.43%    2100.00  增持964.32
    1  深圳市国际信托投资公司  7.03%    1751.86  增持804.91
    2  深圳市社会劳动保险公司  5.14%    1280.30  增持552.24
    3     中电深圳工贸公司  4.11%    1025.25  增持471.06
    4       深圳城建集团  1.82%     452.52  增持207.92
    5      深圳市农业银行  1.03%     256.41   增持87.86
    6     海南南华证券公司  0.83%     205.72   增持94.52
    7     上海万国证券公司  0.67%     168.00   减持21.50
    8      深圳市实验学校  0.63%     157.80        新进
    9       广东证券公司  0.58%     145.12        新进
    """
    assert type_ in ('c', 't')
    query_date_str = pd.Timestamp(query_date).strftime('%Y-%m-%d')
    url_fmt = 'http://quotes.money.163.com/service/{}.html?{}date={}%2C{}&symbol={}#01d02'
    query_type = 'gdfx'
    prefix = 'lt' if type_ == 'c' else ''
    periods = fetch_report_periods(stock_code, type_)
    if query_date_str not in periods.keys():
        raise NoWebData('不存在股票{}报告期为："{}"的股东数据'.format(stock_code,
                                                       query_date_str))
    from_date_str = periods[query_date_str].split(',')[1]
    url = url_fmt.format(query_type, prefix, query_date_str, from_date_str,
                         stock_code)
    # df = pd.read_html(url, encoding='utf-8', header=0, skiprows=range(1))[0]
    attrs = {'class': 'table_bg001 border_box limit_sale'}
    # 必须使用html5lib解析
    df = pd.read_html(url, encoding='utf-8', attrs=attrs, flavor='html5lib')[0]
    return df


@friendly_download()
def fetch_jjcg(stock_code, query_date):
    """
    给定股票代码、期末日期，返回基金持股数据

    Parameters
    ----------
    stock_code : str
      股票代码（6位）

    query_date : date_like
      要查询的期末日期

    Returns
    -------
    res : DataFrame
        包含6列，如案例所示
    Notes
    -----
        如果查询日期不在可选期间，会报错

    Example
    -------
    >>> fetch_jjcg('000001','2004-09-30')
            基金简称  持仓市值(万元)  持仓股数(万股) 与上期持仓股数变化(万股) 占基金净值比例 占流通股比例
    0  融通深证100指数A      3361    390.78       增仓87.12   4.81%  0.28%
    1  博时沪深300指数A      5270    612.78       增仓44.67   1.56%  0.43%
    2        基金普丰     11840   1392.89      减仓800.00   4.32%  0.99%
    3  华宝兴业宝康配置混合         0      0.00            退出       0      0

    """
    query_date_str = pd.Timestamp(query_date).strftime('%Y-%m-%d')
    url_fmt = 'http://quotes.money.163.com/service/{}.html?{}date={}%2C{}&symbol={}'
    query_type = 'jjcg'
    prefix = ''
    periods = fetch_report_periods(stock_code, query_type)
    if query_date_str not in periods.keys():
        raise NoWebData('不存在股票{}报告期为："{}"的基金持股数据'.format(
            stock_code, query_date_str))
    from_date_str = periods[query_date_str].split(',')[1]
    url = url_fmt.format(query_type, prefix, query_date_str, from_date_str,
                         stock_code)
    response = get_page_response(url)
    table = response.json()
    df = pd.read_html(table['table'], header=0, skiprows=range(1))[0]
    return df


def fetch_margin_data(query_date):
    """获取指定日期的市场总体融资融券数据

    Parameters
    ----------

    query_date : date_like
        要查询的日期

    note:
    -------
        如查询日期无数据，返回空表

    Example
    -------
    >>> df = fetch_margin_data('2017-11-07')
    >>> df.loc[:5, ['股票代码','融资余额','融券余额','融券余量']]
         股票代码        融资余额        融券余额     融券余量
    0  300498   515907967   517311017    55000
    1  300355  2014140965  2028052575  1099732
    2  300274   639182457   656585791   893395
    3  300273   956476858   957896458   136500
    4  300257   627784363   627855908     4100
    5  300253   986704177   993525871   923098
    """
    query_date = pd.Timestamp(query_date)
    if query_date.date() >= pd.Timestamp('today').date():
        raise NoWebData('无法获取当天的融资融券数据')
    if query_date.date() < MARGIN_START:
        raise NoWebData('融资融券开始于{}，此前不存在数据。'.format(MARGIN_START))
    url_fmt = "http://quotes.money.163.com/data/margintrade,{}.html"
    date_str = query_date.strftime('%Y%m%d')
    url = url_fmt.format(date_str)
    df = pd.read_html(url)[2].iloc[:, _WY_MARGIN_DATA_USE_COLS]
    df.columns = _WY_MARGIN_DATA_COL_NAMES
    df.insert(1, '日期', query_date.date())
    return df
