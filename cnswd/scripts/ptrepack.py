"""

TODO:废弃

执行频率：
    月

当数据存储在D盘时，ptrepack执行时在文件路径表达上似乎有问题。

变通处理：
1. 拷贝到C用户目录下
2. 执行压缩，输出文件名尾缀 _out
3. 移会源目标目录（覆盖）

"""
import os
import subprocess
import warnings
from pathlib import Path
from shutil import copy2, move

import click
import pandas as pd

warnings.filterwarnings('ignore')


def _cmd(in_p, out_p):
    in_f = in_p.as_posix().replace("C:", "")
    out_f = out_p.as_posix().replace("C:", "")
    return [
        "ptrepack", "--chunkshape=auto", "--propindexes", "--complevel=9",
        "--complib=blosc:blosclz", in_f, out_f
    ]


def do_comp(c):
    """压缩类数据库文件"""
    # 置于此上下文，确保不并行使用数据库
    with c() as store:
        src = store.file_path
        # 复制用户目录下
        des = Path.home() / src.name
        copy2(str(src), str(des))
        out = des.with_name(f"{des.stem}_out.h5")
        cmd = _cmd(des, out)
        cp = subprocess.run(cmd)
        if cp.returncode == 0:
            move(str(out), str(src))
        des.unlink()


def main():
    """压缩数据"""
    with click.progressbar(classes,
                           length=len(classes),
                           show_eta=True,
                           item_show_func=lambda x: f"文件名:{x.__name__}"
                           if x is not None else '完成',
                           label="压缩数据") as pbar:
        for c in pbar:
            try:
                do_comp(c)
            except Exception as e:
                print(f"{c.__name__} \n {e}")
