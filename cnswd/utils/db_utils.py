import pandas as pd


def ingnore_null_dict(d):
    """忽略字典中值为空白的部分

    Args:
        d (dict): 传入字典

    Returns:
        dict: 不含空白值的字典
    """
    res = {}
    for key, value in d.items():
        if pd.notnull(value):
            res[key] = value
    return res


def to_dict(df):
    """将DataFram转换为mongo格式 

    Args:
        df (DataFram): 传入数据对象

    Returns:
        DataFram: 字典格式数据
    """
    data = df.to_dict('records')
    return list(map(ingnore_null_dict, data))
