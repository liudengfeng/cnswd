import warnings

import pandas as pd

from ..cninfo import ClassifyTree
from ..cninfo.classify_tree import PLATE_LEVELS, PLATE_MAPS
from ..store import ClassifyTreeStore

warnings.filterwarnings('ignore')


def _get_classify_tree(api):
    dfs = []
    for i in range(1, 7):
        df = api.get_classify_tree(i)
        df['分类说明'] = PLATE_MAPS[PLATE_LEVELS[i]]
        dfs.append(df)
    return pd.concat(dfs)


def refresh():
    with ClassifyTree(False) as api:
        bom = api.classify_bom
        tree = _get_classify_tree(api)
    # 一次性写入，无需指定`min_itemsize`
    # 采用覆盖式刷新
    with ClassifyTreeStore() as store:
        store.put(bom, 'bom')
        store.put(tree, 'df')
