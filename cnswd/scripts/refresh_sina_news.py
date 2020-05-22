from ..utils import ensure_dtypes
from ..store import SinaNewsStore
from ..websource.sina_news import Sina247News


KW = {
    'min_itemsize': {'概要': 1989}  # 历史最长长度
}
col_dtypes = {
    'd_cols': ['时间'],
    's_cols': ['概要', '分类'],
    'i_cols': ['序号'],
}


def refresh(pages):
    with Sina247News() as api:
        df = api.history_news(pages)
    if df is None or df.empty:
        return
    df = ensure_dtypes(df, **col_dtypes)
    old_max_id = SinaNewsStore.get_attr('max_id', 0)
    new_max_id = df['序号'].max()
    cond = df['序号'] > old_max_id
    to_add = df[cond]
    to_add.drop_duplicates(subset=['序号'], inplace=True)
    print(f"添加{len(to_add)}行")
    SinaNewsStore.append(to_add, KW)
    SinaNewsStore.set_attr('max_id', max(old_max_id, new_max_id))
    SinaNewsStore.create_table_index(None)
