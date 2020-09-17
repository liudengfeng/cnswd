"""
申万行业分类网址：
http://www.swsindex.com/idx0530.aspx

方法：
1. 点击【全部行业分类下载】下载文件
2. 文件默认位置当前下载目录
3. 转换为标准xlsx格式文件
4. 读取数据
"""
import os
from os.path import expanduser, join
from shutil import move

import pandas as pd
MSG = "请访问【http://www.swsindex.com/idx0530.aspx】\n点击【全部行业分类下载】下载文件至{}"
MSG += "使用Excel读取文件，转换为标准excel文件格式，文件名为：SwClass.xlsx。"
MSG += "务必使用excel转换为标准格式，否则会丢失数据！！！"


def get_sw_class(remove=False):
    download_path = os.path.join(expanduser('~'), 'Downloads')
    file_path = join(download_path, 'SwClass.xlsx')
    if not os.path.exists(file_path):
        print(MSG.format(file_path))
        raise FileNotFoundError(f"不存在文件：{file_path}")
    df = pd.read_excel(file_path)
    df.drop(columns=['结束日期'], inplace=True)
    df['股票代码'] = df['股票代码'].map(lambda x: str(x).zfill(6))
    df['起始日期'] = pd.to_datetime(df['起始日期'])
    if remove:
        os.remove(file_path)
    return df
