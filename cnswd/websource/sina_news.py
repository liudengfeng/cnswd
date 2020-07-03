"""新浪24*7财经新闻
"""
import time
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
import pandas as pd

from .._seleniumwire import make_headless_browser
from ..setting.constants import MAX_WORKER
from ..utils import make_logger

logger = make_logger('新浪财经新闻')

TOPIC_MAPS = {
    0: '全部',
    1: '宏观',
    2: '行业',
    3: '公司',
    4: '数据',
    5: '市场',
    6: '观点',
    7: '央行',
    8: '其他',
    10: 'A股',
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
                time.sleep(0.1)
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

    @property
    def live_data(self):
        """最新消息，默认滚动三次"""
        res = []
        for tag in TOPIC_MAPS.keys():
            res.extend(self._get_topic_news(tag, times=3))
        return _to_dataframe(res)

    def _get_topic_news(self, tag, times):
        """获取分类消息"""
        if tag == 0:
            url = self.base_url
        else:
            url = self.base_url + f"?tag={tag}"
        self.driver.get(url)
        # 每个栏目都需要关闭
        self.turn_off()
        div_css = 'div.bd_i'
        for i in range(times):
            if (i + 1) % 100 == 0:
                time.sleep(0.2)
            self.scrolling()
            time.sleep(0.1)
            logger.info(f'当前栏目：{TOPIC_MAPS[tag]:>6} 第{i+1:>4}页')
        # 滚动完成后，一次性读取div元素
        divs = self.driver.find_elements_by_css_selector(div_css)
        return [self._parse(div, tag) for div in divs]

    def history_news(self, pages):
        """历史财经新闻(一次性)"""
        func = partial(self._get_topic_news, times=pages)
        with ThreadPoolExecutor(MAX_WORKER) as pool:
            dfs = pool.map(func, TOPIC_MAPS.keys())
        df = pd.concat(dfs, sort=True, ignore_index=True)
        return df

    def pipeout(self, pages):
        """历史财经新闻（迭代输出）"""
        # `全部`与分项重叠
        for tag in TOPIC_MAPS.keys():
            # logger.info(f'当前栏目：{TOPIC_MAPS[tag]:>8}')
            res = self._get_topic_news(tag, pages)
            yield _to_dataframe(res)
