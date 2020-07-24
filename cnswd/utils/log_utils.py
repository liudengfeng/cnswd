import logging
from log4mongo.handlers import MongoHandler
from ..setting.constants import DB_HOST


def make_logger(name, collection='cnswd', level=logging.NOTSET):
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s %(message)s')
    logger = logging.Logger(name)
    logger.addHandler(
        MongoHandler(level=level,
                     host=DB_HOST,
                     database_name='eventlog',
                     collection=collection if collection else name,
                     fail_silently=False,
                     capped_max=10000,
                     capped=True))
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger
