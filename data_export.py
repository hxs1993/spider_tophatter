# _*_ coding:utf-8 _*_
__author__ = 'hxs'
__date__ = '2019/11/27 18:12'

import json
from config import MongoConnectDB

DB = MongoConnectDB()



all_info = {}
key = 0

class DataExport:

    all_info = {}
    key = 0
    def __init__(self):
        self.fashion_data = DB.fashion_data
        self.jewelry_data = DB.jewelry_data
        self.premium_data = DB.premium_data
        self.home_data = DB.home_data
        self.electronics_data = DB.electronics_data

    def base(self,table):
        for item in table.find():
            data = item['data']
            url = item['url']
            all_info[self.key] = {'data':data,'url':url}
            self.key += 1

    def to_json(self):
        self.base(table=self.jewelry_data)
        self.base(table=self.premium_data)
        self.base(table=self.fashion_data)
        self.base(table=self.home_data)
        self.base(table=self.electronics_data)
        with open('tophatter_data.json', 'w', encoding='utf-8') as f:
            json.dump(all_info, f)

if __name__ == '__main__':
    de = DataExport()
    de.to_json()





