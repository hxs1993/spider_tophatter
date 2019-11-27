# _*_ coding:utf-8 _*_
__author__ = 'hxs'
__date__ = '2019/7/14 22:44'

import pymongo
import requests
from time import sleep
import json
import threading

class MongoConnectDB:
    # fashion
    fashion_links = None
    fashion_data = None
    fashion_attention = None
    fashion_url = 'https://tophatter.com/api/v1/slots/universal.json?category_ids=fashion'
    fashion_type = 'fashion'
    # jewelry
    jewelry_links = None
    jewelry_data = None
    jewelry_attention = None
    jewelry_url = 'https://tophatter.com/api/v1/slots/universal.json?category_ids=jewelry'
    jewelry_type = 'jewelry'
    # premium
    premium_links = None
    premium_data = None
    premium_attention = None
    premium_url = 'https://tophatter.com/api/v1/slots/universal.json?category_ids=premium_products'
    premium_type = 'premium'
    # home
    home_links = None
    home_data = None
    home_attention = None
    home_url = 'https://tophatter.com/api/v1/slots/universal.json?category_ids=home'
    home_type = 'home'
    # electronics
    electronics_links = None
    electronics_data = None
    electronics_attention = None
    electronics_url = 'https://tophatter.com/api/v1/slots/universal.json?category_ids=electronics'
    electronics_type = 'electronics'

    def __init__(self,host='localhost'):
        '''
        默认连接本地MongoDB数据库
        :param host:
        '''
        client = pymongo.MongoClient(host,27017)
        # fashion
        fashion = client['fashion']
        self.fashion_links = fashion['good_links']
        self.fashion_data = fashion['good_data']
        self.fashion_attention = fashion['attention']
        # jewelry
        jewelry = client['jewelry']
        self.jewelry_links = jewelry['good_links']
        self.jewelry_data = jewelry['good_data']
        self.jewelry_attention = jewelry['attention']
        # premium
        premium_products = client['premium_products']
        self.premium_links = premium_products['good_links']
        self.premium_data = premium_products['good_data']
        self.premium_attention = premium_products['attention']
        # home
        home = client['home']
        self.home_links = home['good_links']
        self.home_data = home['good_data']
        self.home_attention = home['attention']
        # electronics
        electronics = client['electronics']
        self.electronics_links = electronics['good_links']
        self.electronics_data = electronics['good_data']
        self.electronics_attention = electronics['attention']


class Tophatter:
    headers = {
        'User_Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
    }

    def __init__(self):
        self.DB = MongoConnectDB()

    def parse(self,url):
        text = requests.get(url=url, headers=self.headers, timeout=60).text
        return json.loads(text)

    def product_url(self,start_url):
        original = self.parse(url=start_url)
        global_analytics = original['config']['analytics']
        slots = original['slots']
        for slot in slots:
            base_url = 'https://tophatter.com/api/v1/lots/{0}.json?category_filters={1}&page={2}&'
            local_analytics = slot['analytics']
            lot_id = ''
            tail_url = []
            for lk, lv in local_analytics.items():
                if lk == 'lot_id':
                    lot_id = lv
                elif lk == 'state':
                    continue
                elif lk == 'amount':
                    continue
                tail_url.append('{}={}'.format(lk, lv))
            url = base_url.format(lot_id, global_analytics['category_filters'], global_analytics['page']) + '&'.join(
                tail_url)
            yield url

    def machine_url(self,start_url,good_links,spider_type):
        while True:
            try:
                urls = self.product_url(start_url)
                for url in urls:
                    if good_links.find_one({'url': url}):
                        print('##############Deduplication##############')
                        continue
                    else:
                        print('%s: %s'%(spider_type,url))
                        good_links.insert_one({'url': url})
                sleep(60)
            except:
                print('*************Timeout*************')
                pass

    def spider(self):
        jewelry = threading.Thread(target=self.machine_url,args=(self.DB.jewelry_url,self.DB.jewelry_links,self.DB.jewelry_type,))
        fashion = threading.Thread(target=self.machine_url,args=(self.DB.fashion_url,self.DB.fashion_links,self.DB.fashion_type,))
        premium = threading.Thread(target=self.machine_url,args=(self.DB.premium_url,self.DB.premium_links,self.DB.premium_type,))
        home = threading.Thread(target=self.machine_url,args=(self.DB.home_url,self.DB.home_links,self.DB.home_type,))
        electronics = threading.Thread(target=self.machine_url,args=(self.DB.electronics_url,self.DB.electronics_links,self.DB.electronics_type,))

        jewelry.start()
        fashion.start()
        premium.start()
        home.start()
        electronics.start()

if __name__ == '__main__':
    tophatter = Tophatter()
    tophatter.spider()
