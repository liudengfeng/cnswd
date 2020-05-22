import shutil
from pathlib import Path
from .path_utils import data_root


def remove_temp_files():
    """删除日志、缓存文件"""
    dirs = ['geckordriver', 'webcache', 'download', 'log']
    paths = [data_root(d) for d in dirs]
    p = Path.home() / '.seleniumwire'
    paths.append(p)
    for p in paths:
        try:
            shutil.rmtree(p)
        except (PermissionError, FileNotFoundError):
            # 可能后台正在使用中，忽略
            pass
