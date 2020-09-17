from cnswd.websource.sw import get_sw_class
from cnswd.mongodb import get_db


def refresh():
    db = get_db()
    collection = db['申万行业分类']
    df = get_sw_class()
    collection.drop()
    collection.insert_many(df.to_dict('records'))
