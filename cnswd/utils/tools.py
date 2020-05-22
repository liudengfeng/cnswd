import os
import subprocess
from collections.abc import Iterable
from io import StringIO
from urllib.parse import urlparse

import pandas as pd
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFPageInterpreter, PDFResourceManager
from pdfminer.pdfpage import PDFPage


def ensure_list(x):
    """
    确保输入参数转换为`list`

    Parameters
    ----------
    x : object
        输入

    Returns
    -------
    res : list
        将输入转换为list

    Notes
    -------
                避免无意义的单一字符循环

    Example
    -------
    >>> ensure_list('000001')
    ['000001']
    >>> ensure_list(('000001','000002'))
    ['000001', '000002']
    """
    if isinstance(x, str):
        return [x]
    elif pd.core.dtypes.common.is_number(x):
        return [x]
    elif isinstance(x, Iterable):
        return [v for v in x]
    else:
        raise TypeError('输入参数"x"要么为str对象，要么为可迭代对象。')


def get_exchange_from_code(stock_code):
    """股票市场分类"""
    if stock_code[:3] == '688':
        return '科创板'
    f = stock_code[0]
    if f == '2':
        return '深市B'
    elif f == '3':
        return '创业板'
    elif f == '6':
        return '沪市A'
    elif f == '9':
        return '沪市B'
    elif stock_code[:3] == '002':
        return '中小板'
    return '深主板A'


def to_plural(word):
    """转换为单词的复数"""
    word = word.lower()
    if word.endswith('y'):
        return word[:-1] + 'ies'
    elif word[-1] in 'sx' or word[-2:] in ['sh', 'ch']:
        return word + 'es'
    elif word.endswith('an'):
        return word[:-2] + 'en'
    else:
        return word + 's'


def filter_a(codes):
    """过滤A股代码"""
    codes = ensure_list(codes)
    return [x for x in codes if x[0] in ('0', '3', '6')]


def is_connectivity(server):
    """判断网络是否连接"""
    fnull = open(os.devnull, 'w')
    result = subprocess.call('ping ' + server + ' -c 2',
                             shell=True, stdout=fnull, stderr=fnull)
    if result:
        res = False
    else:
        res = True
    fnull.close()
    return res


def get_pdf_text(fname, pages=None):
    """读取pdf文件内容"""
    if not pages:
        pagenums = set()
    else:
        pagenums = set(pages)

    output = StringIO()
    manager = PDFResourceManager()
    converter = TextConverter(manager, output, laparams=LAParams())
    interpreter = PDFPageInterpreter(manager, converter)

    infile = open(fname, 'rb')
    for page in PDFPage.get_pages(infile, pagenums):
        interpreter.process_page(page)
    infile.close()
    converter.close()
    text = output.getvalue()
    output.close
    return text


def get_server_name(url):
    """获取主机网络地址

    Arguments:
        url {string} -- 网址

    Returns:
        string -- 返回主机地址
    """
    return urlparse(url)[1]
