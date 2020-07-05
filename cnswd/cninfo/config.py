# 项目名称 | 最早开始日期 | 字段名称 | 是否与股票代码组合成为唯一索引
CN_INFO_CONFIG = {
    '融资融券明细': ('2010-03-31', '交易日期', True),
    # 1
    '基本资料': ('1990-12-09', '上市日期', True),
    # 2
    '公司股东实际控制人': ('2006-12-31', '变动日期', False),
    '公司股本变动': ('2006-12-31', '变动日期', False),
    '上市公司高管持股变动': ('2006-09-30', '公告日期', False),
    '股东增（减）持情况': ('2006-09-30', '增（减）持截止日', False),
    '持股集中度': ('1997-06-30', '截止日期', False),
    # 3
    '投资评级': ('2003-01-02', '发布日期', False),
    # 4
    '上市公司业绩预告': ('2001-06-30', '报告年度', False),
    # 5
    '分红指标': ('1990-12-09', '分红年度', False),
    # 6
    '公司增发股票预案': ('1996-11-29', '公告日期', False),
    '公司增发股票实施方案': ('1996-11-29', '公告日期', False),
    '公司配股预案': ('1993-03-13', '公告日期', False),
    '公司配股实施方案': ('1993-03-13', '公告日期', False),
    '公司首发股票': ('1990-12-09', '上网发行日期', False),
    # 7 报告期
    '个股报告期资产负债表': ('1990-12-09', '报告年度', True),
    '个股报告期利润表': ('1990-12-09', '报告年度', True),
    '个股报告期现金表': ('1998-01-01', '报告年度', True),
    '金融类资产负债表2007版': ('2006-03-31', '报告年度', True),
    '金融类利润表2007版': ('2006-03-31', '报告年度', True),
    '金融类现金流量表2007版': ('2006-03-31', '报告年度', True),
    # 7 指标
    '个股报告期指标表': ('1990-12-09', '报告年度', True),
    '财务指标行业排名': ('1990-12-09', '报告年度', False),
    # 7 单季
    '个股单季财务利润表': ('1990-12-09', '报告年度', True),
    '个股单季现金流量表': ('1998-01-01', '报告年度', True),
    '个股单季财务指标': ('1990-12-09', '报告年度', True),
    # 7 TTM
    '个股TTM财务利润表': ('1990-12-09', '报告年度', True),
    '个股TTM现金流量表': ('1998-01-01', '报告年度', True),
}

ASR_KEYS = [key for key in CN_INFO_CONFIG.keys() if key not in ("融资融券明细")]