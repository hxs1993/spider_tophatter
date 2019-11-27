# _*_ coding:utf-8 _*_
__author__ = 'hxs'
__date__ = '2019/7/24 22:57'

from config import MongoConnectDB
from datetime import datetime
from tqdm import tqdm
import threading

class AttentionDataInsert:
    jewelry_attention = None
    fashion_attention = None
    premium_attention = None
    home_attention = None
    electronics_attention = None

    def __init__(self,host='localhost'):
        DB = MongoConnectDB(host=host)
        self.jewelry_attention = DB.jewelry_attention
        self.jewelry_data = DB.jewelry_data
        self.fashion_attention = DB.fashion_attention
        self.fashion_data = DB.fashion_data
        self.premium_attention = DB.premium_attention
        self.premium_data = DB.premium_data
        self.home_attention = DB.home_attention
        self.home_data = DB.home_data
        self.electronics_attention = DB.electronics_attention
        self.electronics_data = DB.electronics_data

        self.extract = ExtractData()

    def attention_from_jewelry_data(self):
        for item in self.extract.extract(self.jewelry_data):
            if item:
                url = item['url']
                if self.jewelry_attention.find_one({'url':url}):
                    continue
                else:
                    self.jewelry_attention.insert_one(item)

    def attention_from_fashion_data(self):
        for item in self.extract.extract(self.fashion_data):
            if item:
                url = item['url']
                if self.fashion_attention.find_one({'url':url}):
                    continue
                else:
                    self.fashion_attention.insert_one(item)

    def attention_from_premium_data(self):
        for item in self.extract.extract(self.premium_data):
            if item:
                url = item['url']
                if self.premium_attention.find_one({'url':url}):
                    continue
                else:
                    self.premium_attention.insert_one(item)

    def attention_from_home_data(self):
        for item in self.extract.extract(self.home_data):
            if item:
                url = item['url']
                if self.home_attention.find_one({'url':url}):
                    continue
                else:
                    self.home_attention.insert_one(item)

    def attention_from_electronics_data(self):
        for item in self.extract.extract(self.electronics_data):
            if item:
                url = item['url']
                if self.electronics_attention.find_one({'url':url}):
                    continue
                else:
                    self.electronics_attention.insert_one(item)

    def extract_all(self):
        jewelry_thread = threading.Thread(target=self.attention_from_jewelry_data)
        fashion_thread = threading.Thread(target=self.attention_from_fashion_data)
        premium_thread = threading.Thread(target=self.attention_from_premium_data)
        home_thread = threading.Thread(target=self.attention_from_home_data)
        electronics_thread = threading.Thread(target=self.attention_from_electronics_data)

        jewelry_thread.start()
        fashion_thread.start()
        premium_thread.start()
        home_thread.start()
        electronics_thread.start()

class ExtractData:
    def __init__(self):
        pass

    def extract(self,good_data):
        for item in tqdm(good_data.find({},no_cursor_timeout=True), desc='load data'):
            try:
                one = item['data']
                # id
                _id = one['id']
                # 标题
                title = one['title']
                # 商品描述
                description = one['description']
                ###图片
                main_image = one['main_image'].replace('thumbnail', 'large')
                img_name = main_image.split('/')[-2] + '.jpg'
                # 起拍价
                starting_bid_amount = float(one['starting_bid_amount'])
                # 拍卖价
                hammer_price = float(one['hammer_price'])
                # 邮费
                shipping_price = float(one['shipping_price'])
                # 店家名称
                seller_name = one['seller_name']
                # 店家销量
                seller_lots_sold = one['seller_lots_sold']
                # 评论次数
                ratings_count = one['ratings_count']
                # 平均评分
                ratings_average = one['ratings_average']
                # 起拍时间
                bidding_started_at = ExtractData.date_to_timestamp(one['bidding_started_at'])
                # 拍出时间
                bidding_ended_at = ExtractData.date_to_timestamp(one['bidding_ended_at'])
                # 拍卖时长
                bidding_time = (bidding_ended_at - bidding_started_at).seconds
                # 链接
                url = item['url']
                info = {
                    'title':title,
                    'description':description,
                    'main_image':main_image,
                    'img_name':img_name,
                    'starting_bid_amount':starting_bid_amount,
                    'hammer_price':hammer_price,
                    'shipping_price':shipping_price,
                    'seller_name':seller_name,
                    'seller_lots_sold':seller_lots_sold,
                    'ratings_count':ratings_count,
                    'ratings_average':ratings_average,
                    'bidding_started_at':bidding_started_at,
                    'bidding_ended_at':bidding_ended_at,
                    'bidding_time':bidding_time,
                    'url':url,
                }
                yield info
            except:
                yield 0

    @staticmethod
    def date_to_timestamp(date):
        return datetime.strptime(date.replace('T', ' ').replace('+08:00', ''), '%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':
    host = 'localhost'
    attention = AttentionDataInsert(host=host)

    # attention.extract_all()

    attention.attention_from_jewelry_data()
    attention.attention_from_fashion_data()
    attention.attention_from_premium_data()
    attention.attention_from_home_data()
    attention.attention_from_electronics_data()