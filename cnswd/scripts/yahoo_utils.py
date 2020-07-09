import json
from os import path
import io

here = path.abspath(path.dirname(__file__))


def get_cname_maps(tname):
    """获取表列名称映射

    Args:
        tname (str): 表名称

    Returns:
        dict: 以英文名称为键，值为中文名称
    """
    fn = f"yahoo/{tname}.json"
    with io.open(path.join(here, fn), mode='r', encoding='utf-8') as f:
        ds = json.loads(f.read())
    return {d['key']: d['value'] for d in ds}
