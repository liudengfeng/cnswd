"""新浪24*7财经新闻
"""
import time
from functools import partial
import pandas as pd

from .._seleniumwire import make_headless_browser
from ..utils import make_logger

logger = make_logger('新浪财经新闻')

TOPIC_MAPS = {
    1: '宏观',
    2: '行业',
    3: '公司',
    4: '数据',
    5: '市场',
    6: '观点',
    7: '央行',
    8: '其他',
    10: 'A股',
    0: '全部',  # 不能分类的，只在全部显示。为提前消息分类，将其放最后
}

COLUMNS = ['序号', '时间', '概要', '分类']


def _to_dataframe(data):
    if len(data):
        data = sorted(data, key=lambda item: item[0], reverse=True)
        df = pd.DataFrame.from_records(data)
        df['时间'] = df[1].astype(str) + ' ' + df[2]
        df['时间'] = pd.to_datetime(df['时间'])
        df.rename(columns={0: '序号', 3: '概要', 4: '分类'}, inplace=True)
        df.drop(columns=[1, 2], inplace=True)
        df.drop_duplicates(subset='序号', inplace=True)
    else:
        df = pd.DataFrame()
    return df


class Sina247News(object):
    def __init__(self):
        self.base_url = 'http://finance.sina.com.cn/7x24/'
        self.driver = make_headless_browser()
        self._off = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.driver.quit()

    def scrolling(self):
        # 每次递增20条
        self.driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")

    def _turn_off(self):
        csses = ['.soundswitch', '.autorefreshlbtxt']
        for css in csses:
            elem = self.driver.find_element_by_css_selector(css)
            if elem.is_selected():
                elem.click()
        self._off = True

    def turn_off(self):
        """关闭声音提醒及自动更新"""
        if not self._off:
            self._turn_off()

    def _parse(self, div, tag):
        """解析单个消息内容"""
        # 编号、日期(20180901)、时间('22:31:44')、概要
        ps = div.find_elements_by_tag_name('p')
        return (
            div.get_attribute('data-id'),
            div.get_attribute('data-time'),
            ps[0].text,
            ps[1].text,
            TOPIC_MAPS[tag],
        )

    def _get_topic_news(self, tag, times):
        """获取分类消息"""
        url = self.base_url + f"?tag={tag}"
        self.driver.get(url)
        # 每个栏目都需要关闭
        self.turn_off()
        div_css = 'div.bd_i'
        for i in range(times):
            self.scrolling()
            time.sleep(0.1)
            logger.info(f'当前栏目：{TOPIC_MAPS[tag]:>6} 第{i+1:>4}页')
            # 滚动完成后，读取div元素
            divs = self.driver.find_elements_by_css_selector(div_css)[-20:]
            res = [self._parse(div, tag) for div in divs]
            yield res

    def yield_history_news(self, pages):
        """历史财经新闻(一次性)"""
        for tag in TOPIC_MAPS.keys():
            for data in self._get_topic_news(tag, pages):
                yield _to_dataframe(data)
