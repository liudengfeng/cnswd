"""

基础配置文件

h5查询字段名称中不得含`（`
"""
from pathlib import Path

from .constants import MARKET_START

DEFAULT_CONFIG = {
    # 数据目录
    'data_root': Path("D://") / '.cnswd',  # 或者 Path.home() / '.cnswd'
    # 驱动程序位置 文件目录使用`/`
    'geckodriver_path': r'C:/tools/geckodriver.exe',
}

LOG_TO_FILE = False        # 是否将日志写入到文件
TIMEOUT = 120              # 最长等待时间，单位：秒。速度偏慢，加大超时时长
# 轮询时间缩短
POLL_FREQUENCY = 0.2

default_start_date = MARKET_START.strftime(r'%Y-%m-%d')
D2D_CSS = ('input.date:nth-child(1)', 'input.form-control:nth-child(2)')
YAQ_CSS = ('#se1_sele', '.condition2 > select:nth-child(2)')


# region 数据浏览器
# 日期包含二部分
# 1. 循环与格式符号
# 2. 是否排除未来日期

# 循环与格式符号说明
# 第一个代表周期，第二个字符代表格式
# QD -> 按季度循环，以日期格式表达
# QQ -> 按季度循环，t1 按年份数 t2 按季度
# 以月度、季度循环都不能排除未来日期
# 例如当日为 xx-05 查询xx月，表达为`xx-01 ~ xx-30`，结果为空集

DB_CONFIG = {
    # 1 基本资料
    '1': {
        'name': '基本资料',
        'css': (None, None),
        'date_freq': (None, None),
        # 'model': StockInfo,
        'date_field': (None, None),
        # 路径关键字
        'api_key': 'api/sysapi/p_sysapi1018',
        'key_columns': ['股票代码', '上市日期'],  # 统一,
        'min_itemsize': {'ISIN代码': 12,
                         '上市地点': 10,
                         '上市状态': 20,
                         '主营业务': 400,
                         '会计师事务所': 50,
                         '公司传真': 50,
                         '公司电子邮件地址': 100,
                         '公司电话': 100,
                         '公司简介': 2000,
                         '公司网站': 100,
                         '办公地址': 100,
                         '城市': 10,
                         '律师事务所': 100,
                         '总经理': 20,
                         '法定代表人': 50,
                         '注册地址': 200,
                         '独立董事': 100,
                         '申万行业一级名称': 4,
                         '申万行业三级名称': 8,
                         '申万行业二级名称': 8,
                         '省份': 3,
                         '经营范围': 2000,
                         '股票简称': 20,
                         '英文名称': 100,
                         '英文简称': 100,
                         '董事会秘书': 50,
                         '董事长': 50,
                         '董秘传真': 100,
                         '董秘电话': 100,
                         '董秘邮箱': 100,
                         '证券事务代表': 50,
                         '证券类别': 2,
                         '证监会一级行业名称': 20,
                         '证监会二级行业名称': 20,
                         '邮编': 6}
    },

    # 2 股本股东
    '2.1': {
        'name': '公司股东实际控制人',
        'css': D2D_CSS,
        'date_freq': ('QD', False),
        # 'model': ActualController,
        'date_field': ('变动日期', '2006-12-31'),
        'api_key': 'api/stock/p_stock2213',
        'key_columns': ['股票代码', '变动日期', '直接持有人名称'],
        'min_itemsize': {'实际控制人ID': 200,
                         '实际控制人名称': 200,
                         '实际控制人类型': 10,
                         '控制类型': 10,
                         '控制类型编码': 6,
                         '直接持有人ID': 200,
                         '直接持有人名称': 600,
                         '直接控制人ID': 100,
                         '直接控制人名称': 300,
                         '股票简称': 20}
    },
    '2.2': {
        'name': '公司股本变动',
        'css': D2D_CSS,
        'date_freq': ('QD', False),
        # 'model': CompanyShareChange,
        'date_field': ('变动日期', '2006-12-31'),
        'api_key': 'api/stock/p_stock2215',
        'key_columns': ['股票代码', '变动日期', '变动原因'],
        'min_itemsize': {'变动原因': 100,
                         '变动原因编码': 50,
                         '股票简称': 20}
    },
    '2.3': {
        'name': '上市公司高管持股变动',
        'css': D2D_CSS,
        'date_freq': ('MD', False),
        # 网站为截止日期，实际为公告日期
        'date_field': ('公告日期', '2006-09-30'),
        'api_key': 'api/stock/p_stock2218',
        'key_columns': ['股票代码', '公告日期', '变动人'],
        'min_itemsize': {'变动人': 100,
                         '变动人与董监高关系': 50,
                         '持股变动原因': 50,
                         '董监高姓名': 50,
                         '董监高职务': 50,
                         '股票简称': 20}
    },
    '2.4': {
        'name': '股东增（减）持情况',
        'css': D2D_CSS,
        'date_freq': ('QD', False),
        # 'model': ShareholderShareChange,
        'date_field': ('增减持截止日', '2006-09-30'),
        'api_key': 'api/stock/p_stock2226',
        'key_columns': ['股票代码', '增减持截止日', '股东名称'],
        'min_itemsize': {'增减持价格上限': 100,
                         '股东名称': 200,
                         '股票简称': 20}
    },
    '2.5': {
        'name': '持股集中度',
        'css': YAQ_CSS,
        'date_freq': ('QQ', False),
        # 'model': ShareholdingConcentration,
        'date_field': ('截止日期',  '1997-06-30'),
        'api_key': 'api/sysapi/p_sysapi1020',
        'key_columns': ['股票代码', '截止日期'],
        'min_itemsize': {'前十大股东': 50, '股票简称': 20}
    },

    # 3 投资评级
    '3': {
        'name': '投资评级',
        'css': D2D_CSS,
        'date_freq': ('MD', False),
        # 'model': InvestmentRating,
        'date_field': ('发布日期',  '2003-01-02'),
        'api_key': 'api/sysapi/p_stock2205',
        'key_columns': ['股票代码', '发布日期', '研究员名称'],
        'min_itemsize': {'前一次投资评级': 10,
                         '投资评级': 20,
                         '投资评级（经调整）': 20,
                         '是否首次评级': 6,
                         '研究员名称': 100,
                         '研究机构简称': 100,
                         '股票简称': 20}
    },

    # 4 业绩预期
    '4.1': {
        'name': '上市公司业绩预告',
        'css': YAQ_CSS,
        'date_freq': ('QQ', False),
        # 'model': PerformanceForecaste,
        'date_field': ('报告年度',  '2001-06-30'),
        'api_key': 'api/stock/p_stock2238',
        'key_columns': ['股票代码', '报告年度'],
        'min_itemsize': {'业绩变化原因': 2000,
                         '业绩预告内容': 2000,
                         '股票简称': 20}
    },

    # 5 分红指标
    '5': {
        'name': '分红指标',
        'css': ('#se2_sele', None),
        'date_freq': ('YY', False),
        # 'model': Dividend,
        'date_field': ('分红年度', default_start_date),
        'api_key': 'api/sysapi/p_sysapi1019',
        'key_columns': ['股票代码', '分红年度'],
        'min_itemsize': {'实施方案分红说明': 1000,
                         '股东大会预案分红说明': 1000,
                         '股票简称': 20,
                         '董事会预案分红说明': 1000}
    },

    # 6 筹资指标
    '6.1': {
        'name': '公司增发股票预案',
        'css': D2D_CSS,
        'date_freq': ('QD', False),
        # 'model': AdditionalStockPlan,
        'date_field': ('公告日期',  '1996-11-29'),
        'api_key': 'api/stock/p_stock2229',
        'key_columns': ['股票代码', '公告日期'],
        'min_itemsize': {'发行定价方式': 1000,
                         '发行对象': 500,
                         '变更说明': 500,
                         '股票简称': 20,
                         '预计募资投向': 2000}
    },
    '6.2': {
        'name': '公司增发股票实施方案',
        'css': D2D_CSS,
        'date_freq': ('QD', False),
        # 'model': AdditionalStockImplementation,
        'date_field': ('公告日期',  '1996-11-29'),
        'api_key': 'api/stock/p_stock2230',
        'key_columns': ['股票代码', '公告日期'],
        'min_itemsize': {'分配承诺': 100,
                         '发行定价方式': 1000,
                         '发行对象': 500,
                         '招股意向书网址': 100,
                         '股票简称': 20}
    },
    '6.3': {
        'name': '公司配股预案',
        'css': D2D_CSS,
        'date_freq': ('QD', False),
        # 'model': SharePlacementPlan,
        'date_field': ('公告日期', '1993-03-13'),
        'api_key': 'api/stock/p_stock2231',
        'key_columns': ['股票代码', '公告日期'],
        'min_itemsize': {'变更说明': 1000,
                         '股东大会预案内容': 500,
                         '股票简称': 20,
                         '董事会预案内容': 500,
                         '配股价格确定方式': 500,
                         '预计募资投向': 2000}
    },
    '6.4': {
        'name': '公司配股实施方案',
        'css': D2D_CSS,
        'date_freq': ('QD', False),
        # 'model': SharePlacementImplementation,
        'date_field': ('公告日期', '1993-03-13'),
        'api_key': 'api/stock/p_stock2232',
        'key_columns': ['股票代码', '公告日期'],
        'min_itemsize': {'大股东认购方式': 1000,
                         '配售对象': 500}
    },
    '6.5': {
        'name': '公司首发股票',
        'css': (None, None),
        'date_freq': (None, None),
        # 'model': IPO,
        'date_field': (None, None),
        'api_key': 'api/stock/p_stock2233',
        'key_columns': ['股票代码', '上网发行日期'],
        'min_itemsize': {'分配承诺': 500,
                         '发行对象': 500,
                         '发行方式': 100,
                         '股票简称': 20}
    },

    # 7 财务指标

    # 7.1 报告期
    '7.1.1': {
        'name': '个股报告期资产负债表',
        'css': YAQ_CSS,
        'date_freq': ('QQ', False),
        # 'model': PeriodlyBalanceSheet,
        'date_field': ('报告年度', default_start_date),
        'api_key': 'api/stock/p_stock2300',
        'key_columns': ['股票代码', '报告年度'],
        'min_itemsize': {},
    },
    '7.1.2': {
        'name': '个股报告期利润表',
        'css': YAQ_CSS,
        'date_freq': ('QQ', False),
        # 'model': PeriodlyIncomeStatement,
        'date_field': ('报告年度', default_start_date),
        'api_key': 'api/stock/p_stock2301',
        'key_columns': ['股票代码', '报告年度'],
        'min_itemsize': {},
    },
    '7.1.3': {
        'name': '个股报告期现金表',
        'css': YAQ_CSS,
        'date_freq': ('QQ', False),
        # 'model': PeriodlyCashFlowStatement,
        'date_field': ('报告年度', default_start_date),
        'api_key': 'api/stock/p_stock2302',
        'key_columns': ['股票代码', '报告年度'],
        'min_itemsize': {},
    },
    # '7.1.4': {
    #     'name': '金融类资产负债表2007版',
    #     'css': YAQ_CSS,
    #     'date_freq': ('QQ', False),
    #     # 'model': PeriodlyBalanceSheet2007,
    #     'date_field': ('报告年度', '2006-03-31'),
    #     'api_key': 'api/stock/p_stock2325',
    #     'key_columns': ['股票代码', '报告年度'],
    #     'min_itemsize': {},
    # },
    # '7.1.5': {
    #     'name': '金融类利润表2007版',
    #     'css': YAQ_CSS,
    #     'date_freq': ('QQ', False),
    #     # 'model': PeriodlyIncomeStatement2007,
    #     'date_field': ('报告年度', '2006-03-31'),
    #     'api_key': 'api/stock/p_stock2326',
    #     'key_columns': ['股票代码', '报告年度'],
    #     'min_itemsize': {},
    # },
    # '7.1.6': {
    #     'name': '金融类现金流量表2007版',
    #     'css': YAQ_CSS,
    #     'date_freq': ('QQ', False),
    #     # 'model': PeriodlyCashFlowStatement2007,
    #     'date_field': ('报告年度', '2006-03-31'),
    #     'api_key': 'api/stock/p_stock2327',
    #     'key_columns': ['股票代码', '报告年度'],
    #     'min_itemsize': {},
    # },

    # 7.2 财务指标
    '7.2.1': {
        'name': '个股报告期指标表',
        'css': YAQ_CSS,
        'date_freq': ('QQ', False),
        # 'model': PeriodlyFinancialIndicator,
        'date_field': ('报告年度', '1991-12-31'),
        'api_key': 'api/stock/p_stock2303',
        'key_columns': ['股票代码', '报告年度'],
        'min_itemsize': {},
    },
    '7.2.2': {
        'name': '财务指标行业排名',
        'css': YAQ_CSS,
        'date_freq': ('QQ', False),
        # 'model': FinancialIndicatorRanking,
        'date_field': ('报告期', '1991-12-31'),
        'api_key': 'api/stock/p_stock2501',
        'key_columns': ['股票代码', '报告期'],
        'min_itemsize': {},
    },

    # 7.3 单季度
    '7.3.1': {
        'name': '个股单季财务利润表',
        'css': YAQ_CSS,
        'date_freq': ('QQ', False),
        # 'model': QuarterlyIncomeStatement,
        'date_field': ('报告年度', default_start_date),
        'api_key': 'api/stock/p_stock2329',
        'key_columns': ['股票代码', '报告年度'],
        'min_itemsize': {},
    },
    '7.3.2': {
        'name': '个股单季现金流量表',
        'css': YAQ_CSS,
        'date_freq': ('QQ', False),
        # 'model': QuarterlyCashFlowStatement,
        'date_field': ('报告年度', default_start_date),
        'api_key': 'api/stock/p_stock2330',
        'key_columns': ['股票代码', '报告年度'],
        'min_itemsize': {},
    },
    '7.3.3': {
        'name': '个股单季财务指标',
        'css': YAQ_CSS,
        'date_freq': ('QQ', False),
        # 'model': QuarterlyFinancialIndicator,
        'date_field': ('报告年度', default_start_date),
        'api_key': 'api/stock/p_stock2331',
        'key_columns': ['股票代码', '报告年度'],
        'min_itemsize': {},
    },
    # 7.4 TTM
    '7.4.1': {
        'name': '个股TTM财务利润表',
        'css': YAQ_CSS,
        'date_freq': ('QQ', False),
        # 'model': TtmIncomeStatement,
        'date_field': ('报告年度', default_start_date),
        'api_key': 'api/stock/p_stock2332',
        'key_columns': ['股票代码', '报告年度'],
        'min_itemsize': {},
    },
    '7.4.2': {
        'name': '个股TTM现金流量表',
        'css': YAQ_CSS,
        'date_freq': ('QQ', False),
        # 'model': TtmCashFlowStatement,
        'date_field': ('报告年度', '1998-01-01'),
        'api_key': 'api/stock/p_stock2333',
        'key_columns': ['股票代码', '报告年度'],
        'min_itemsize': {},
    },
}
# endregion

# region 专题统计
# 名称
# css
# 时间_1_css 时间_2_css 选项_css
# model
# 字段
# 如数据浏览器中有类似表，忽略专题统计
# 如有明细表，则忽略统计表

DT1_CSS = '#fBDatepair > input:nth-child(1)'
OPT_CSS = '.condition6 > select:nth-child(2)'

TS_CONFIG = {
    # 1 股东股本
    '1.1': {
        'name': '解禁报表',
        'css': (DT1_CSS, None, None),
        'date_freq': ('DD', False),
        # 'model': Deregulation,
        'date_field': ('实际解除限售日期', '2007-09-17'),
    },
    '1.2': {
        'name': '减持明细',
        'css': (DT1_CSS, None, None),
        'date_freq': ('DD', False),
        # 'model': Underweight,
        'date_field': ('公告日期', '2007-04-30'),
    },
    '1.3': {
        'name': '增持明细',
        'css': (DT1_CSS, None, None),
        'date_freq': ('DD', False),
        # 'model': Overweight,
        'date_field': ('公告日期', '2006-09-30'),
    },
    '1.4': {
        'name': '减持汇总统计',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '1.5': {
        'name': '增持汇总统计',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '1.6': {
        'name': '股本变动',
        'css': (None, None, OPT_CSS),
        'date_freq': (None, None),
        # 'model': Equity,
        'date_field': ('公告日期', default_start_date),
    },
    '1.7': {
        'name': '高管持股变动明细',
        'css': (None, None, OPT_CSS),  # DT1使用默认第一项 分项选择
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '1.8': {
        'name': '高管持股变动汇总',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '1.9': {
        'name': '实际控制人持股变动',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '1.10': {
        'name': '股东人数及持股集中度',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },

    # 2 业绩与分红
    '2.1': {
        'name': '业绩预告',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '2.2': {
        'name': '预告业绩扭亏个股',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '2.3': {
        'name': '预告业绩大幅下降个股',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '2.4': {
        'name': '预告业绩大幅上升个股',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '2.5': {
        'name': '地区分红明细',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '2.6': {
        'name': '行业分红明细',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '2.7': {
        'name': '报告期分红明细',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },

    # # 3 发行筹资
    '3.1': {
        'name': '首发审核',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '3.2': {
        'name': '首发筹资',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '3.3': {
        'name': '增发筹资',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '3.4': {
        'name': '配股筹资',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '3.5': {
        'name': '公司债或可转债',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '3.6': {
        'name': '新股申购',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '3.7': {
        'name': '新股发行',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '3.8': {
        'name': '新股过会',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },

    # 4 公司治理
    '4.1': {
        'name': '资产重组',
        'css': (None, None, OPT_CSS),
        'date_freq': ('MM', False),
        'model': '',
        'date_field': ('公告日期', default_start_date),
    },
    '4.2': {
        'name': '债务重组',
        'css': (None, None, None),
        'date_freq': ('MM', False),
        'model': '',
        'date_field': ('公告日期', default_start_date),
    },
    '4.3': {
        'name': '吸收合并',
        'css': (None, None, OPT_CSS),
        'date_freq': ('YY', False),
        'model': '',
        'date_field': ('公告日期', default_start_date),
    },
    '4.4': {
        'name': '股权变更',
        'css': (None, None, OPT_CSS),
        'date_freq': ('MM', False),
        'model': '',
        'date_field': ('公告日期', default_start_date),
    },
    '4.5': {
        'name': '对外担保',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '4.6': {
        'name': '公司诉讼',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '4.7': {
        'name': '并购重组',
        'css': (DT1_CSS, None, None),
        'date_freq': ('DD', False),
        # 'model': MA,
        'date_field': ('公告日期', default_start_date),
    },
    '4.8': {
        'name': '股票质押',
        'css': (DT1_CSS, None, None),
        'date_freq': ('DD', False),
        # 'model': Pledge,
        'date_field': ('公告日期', '2000-06-24'),
    },

    # 5 财务报表
    '5.1': {
        'name': '个股主要指标',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '5.2': {
        'name': '分地区财务指标',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '5.3': {
        'name': '分行业财务指标',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '5.4': {
        'name': '分市场财务指标',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '5.5': {
        'name': '盈利能力',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '5.6': {
        'name': '运营能力',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '5.7': {
        'name': '成长能力',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '5.8': {
        'name': '偿债能力',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },

    # 6 行业分析
    '6.1': {
        'name': '行业市盈率',
        'css': (DT1_CSS, None, OPT_CSS),
        'date_freq': ('DD', False),
        # 'model': Pe,
        'date_field': ('变动日期', '2013-06-13'),
    },

    # # 7 评价预测
    '7.1': {
        'name': '投资评级',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },

    # 8 市场交易
    '8.1': {
        'name': '大宗交易报表',
        'css': ('#fBDatepair > input:nth-child(1)', None, None),
        'date_freq': ('DD', False),
        # 'model': BigDeal,
        'date_field': ('交易日期', '2000-01-01'),
    },
    '8.2': {
        'name': '融资融券明细',
        'css': ('#fBDatepair > input:nth-child(1)', None, None),
        'date_freq': ('DD', False),
        # 'model': MarginStore,
        'date_field': ('交易日期', '2010-03-31'),
        'api_key': 'api/sysapi/p_sysapi1023',
        'key_columns': ['股票代码', '交易日期'],
    },

    # 9 信息提示
    '9.1': {
        'name': '股东大会召开情况',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '9.2': {
        'name': '股东大会相关事项变动',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '9.3': {
        'name': '股东大会议案表',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '9.4': {
        'name': '停复牌',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '9.5': {
        'name': '市场公开信息汇总',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '9.6': {
        'name': '拟上市公司清单',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '9.7': {
        'name': '暂停上市公司清单',
        'css': (None, None, None),
        'date_freq': (None, None),
        # 'model': Suspend,
        'date_field': (None, None),
    },
    '9.8': {
        'name': '终止上市公司清单',
        'css': (None, None, None),
        'date_freq': (None, None),
        # 'model': Delisted,
        'date_field': (None, None),
    },
    '9.9': {
        'name': '定期报告披露预约时间表',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },

    # 10 基金报表
    '10.1': {
        'name': '基金净值增长率',
        'css': (DT1_CSS, None, OPT_CSS),
        'date_freq': ('DD', False),
        # 'model': Rnav,
        'date_field': ('净值日期', default_start_date),
    },
    '10.2': {
        'name': '上市基金行情',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '10.3': {
        'name': '基金重仓股',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '10.4': {
        'name': '基金行业配置',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '10.5': {
        'name': '基金资产配置',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },

    # # 11 债券报表
    # # 11.1 债券发行
    '11.1.1': {
        'name': '国债发行',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '11.1.2': {
        'name': '地方债发行',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '11.1.3': {
        'name': '企业债发行',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '11.1.4': {
        'name': '可转债发行',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '11.1.5': {
        'name': '可转债转股',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    # 11.2 债券基本信息
    '11.2.1': {
        'name': '债券基本信息查询',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '11.2.2': {
        'name': '债券发行相关中介机构查询',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '11.2.3': {
        'name': '债券信用评级查询',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '11.2.4': {
        'name': '债券发行机构信用评级查询',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '11.2.5': {
        'name': '债券特殊条款及其细项',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '11.2.6': {
        'name': '债券担保信息',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '11.2.7': {
        'name': '债券利率',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '11.2.8': {
        'name': '债券现金流明细',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '11.2.9': {
        'name': '可转债转股',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
    '11.2.10': {
        'name': '可转债转股价格调整',
        'css': '',
        'date_freq': '',
        'model': '',
        'date_field': '',
    },
}
# endregion
