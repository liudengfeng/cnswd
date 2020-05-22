import os
from pathlib import Path

from ..setting.config import DEFAULT_CONFIG


def data_root(sub='data', root=None):
    """根目录下子目录或文件路径

    Notes:
        'a' 代表 子目录
        'a.csv' 代表文件
        以"a/b"表达时，代表二级目录
        以"a/b.csv"表达时，代表二级目录下的文件路径
    """
    has_file = False
    if root is None:
        root = DEFAULT_CONFIG['data_root']
    subs = sub.split('/')
    if len(subs[-1].split('.')) == 1:
        path = root / Path(sub)
    else:
        path = root / Path('/'.join(subs[:-1]))
        has_file = True
    if not path.exists():
        path.mkdir(parents=True)
    if not has_file:
        return path
    else:
        return root / Path('/'.join(subs[:-1])) / subs[-1]


def most_recent_path(file_dir):
    """获取文件目录下最新的文件路径

    Arguments:
        file_dir {str} -- 要寻找的文件目录

    Raises:
        FileNotFoundError -- 文件目录下不存在文件

    Returns:
        [type] -- [description]
    """
    try:
        iterms = os.scandir(file_dir)
        return sorted(iterms, key=lambda x: x.stat().st_ctime, reverse=True)[0].path
    except:
        raise FileNotFoundError('目录：{} 不存在文件'.format(file_dir))
