from cnswd.cninfo.classify_tree import _level_split, _LevelEncoder


def test_with_enddate_1():
    """测试四级分类编码"""
    lencoder = _LevelEncoder()
    l_a_c = {
        '1.1.1.1': 'S110101',
        '1.1.1.2': 'S110102',
        '1.1.1.3': 'S110103',
        '1.1.2.1': 'S110201',
        '1.1.2.2': 'S110202',
        '1.1.3.1': 'S110301',
        '1.1.4.1': 'S110401',
        '1.1.5.1': 'S110501',
        '1.1.5.2': 'S110502',
        '1.1.5.3': 'S110504',
        '1.1.6.1': 'S110601',
        '1.1.7.1': 'S110701',
        '1.1.8.1': 'S110801',
        '1.2.1.1': 'S210101',
        '1.2.2.1': 'S210201',
        '1.2.2.2': 'S210202',
        '1.2.3.1': 'S210301',
    }
    for expected, code in l_a_c.items():
        code_tuple = _level_split(1, code)
        actual = lencoder.encode(code_tuple)
        assert actual == expected


def test_with_enddate_2():
    """测试三级分类编码"""
    lencoder = _LevelEncoder()
    l_a_c = {
        '4.1.1': '110100',
        '4.1.2': '110200',
        '4.2.1': '120100',
        '4.2.2': '120200',
    }
    for expected, code in l_a_c.items():
        code_tuple = _level_split(4, code)
        actual = lencoder.encode(code_tuple)
        assert actual == expected