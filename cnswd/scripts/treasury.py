from ..mongodb import get_db
from ..utils import make_logger
from ..utils.db_utils import to_dict
from ..websource.treasuries import fetch_treasury_data_from, download_last_year

collection_name = '国债利率'
logger = make_logger(collection_name)


def refresh():
    logger.info('......')
    db = get_db()
    collection = db[collection_name]
    collection.drop()
    # 首先下载当年数据
    download_last_year()
    df = fetch_treasury_data_from()
    df.reset_index(inplace=True)
    collection.insert_many(to_dict(df))
    logger.info(f"更新 {len(df)} 行")
