"""
数据备份

频率：
    周

重点数据备份
备份前确认可读，防止将已损坏的文档覆盖目标备份。

"""
import os
from pathlib import Path
from shutil import copy2

import click
import pandas as pd

from ..store import (ClassifyTreeStore, DataBrowseStore, DisclosureStore,
                     MarginStore, SinaNewsStore, SinaQuotesStore,
                     TctMinutelyStore, ThsGnStore, WyCjmxStore,
                     WyStockDailyStore)

classes = (
    MarginStore,
    WyStockDailyStore,
    DisclosureStore,
    SinaQuotesStore,
    ClassifyTreeStore,
    SinaNewsStore,
    WyCjmxStore,
    ThsGnStore,
    TctMinutelyStore,
    # DataBrowseStore,
)


def is_ok(c):
    # 数据可读、且有数据
    store = c.get_store()
    fp = store.filename
    cond1 = os.access(fp, os.R_OK)
    store.close()
    # 判断数据可读
    if c.__name__.lower().startswith('data_'):
        key = '1/df'
    else:
        key = 'df'
    df = pd.read_hdf(fp, key, start=0, stop=1)
    cond2 = len(df) == 1
    return cond1 & cond2


def _backup(c):
    if not is_ok(c):
        return
    store = c.get_store()
    # 源路径与目标路径
    s_p = Path(store.filename)
    store.close()
    t_p = s_p.parent / 'backup'
    if not t_p.exists():
        t_p.mkdir(parents=False)
    # 占用大量内存
    # store.copy(
    #     d_p / s_p.name,
    #     complib=HDF_KWARGS['complib'],
    #     complevel=HDF_KWARGS['complevel'])
    copy2(str(s_p), str(t_p))


def backup():
    """备份数据"""
    with click.progressbar(classes,
                           length=len(classes),
                           show_eta=True,
                           item_show_func=lambda x: f"名称：{x.__name__}" if x is not None else '完成',
                           label="备份数据") as pbar:
        for c in pbar:
            try:
                _backup(c)
            except Exception as e:
                print(f"{c.__name__} \n {e}")
