# _*_ coding:utf-8 _*_
__author__ = 'hxs'
__date__ = '2019/7/4 20:33'

'''
    1.查看各类型商品的数量以及所有类型商品的总数
    2.通过链接下载对应的data数据
    3.通过data数据导出指定商品类型的特征
'''

import pymongo
import requests
import json
from datetime import datetime
import logging
from tqdm import tqdm
import pandas as pd
import os
import threading
from urllib.request import urlretrieve
from time import sleep

logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_PATH,'data')
IMG_PATH = os.path.join(BASE_PATH,'img')

# client = pymongo.MongoClient('localhost', 27017)
client = pymongo.MongoClient('localhost', 27017)
# client = pymongo.MongoClient('108.61.204.4',27017)

# 导入数据与查询数据记录
class DBTables:
    # fashion
    fashion = client['fashion']
    fashion_links = fashion['good_links']
    fashion_data = fashion['good_data']
    fashion_attention = fashion['attention']
    # jewelry
    jewelry = client['jewelry']
    jewelry_links = jewelry['good_links']
    jewelry_data = jewelry['good_data']
    jewelry_attention = jewelry['attention']
    # premium
    premium_products = client['premium_products']
    premium_links = premium_products['good_links']
    premium_data = premium_products['good_data']
    premium_attention = premium_products['attention']
    # home
    home = client['home']
    home_links = home['good_links']
    home_data = home['good_data']
    home_attention = home['attention']
    # electronics
    electronics = client['electronics']
    electronics_links = electronics['good_links']
    electronics_data = electronics['good_data']
    electronics_attention = electronics['attention']

    def count_link(self):
        # fashion
        fashion_nums = self.fashion_links.count()
        print('fashion link nums: %s'%fashion_nums)
        # jewelry
        jewelry_nums = self.jewelry_links.count()
        print('jewelry link nums: %s'%jewelry_nums)
        # premium
        premium_nums = self.premium_links.count()
        print('premium link nums: %s'%premium_nums)
        # home
        home_nums = self.home_links.count()
        print('home link nums: %s'%home_nums)
        # electronics
        electronics_nums = self.electronics_links.count()
        print('electronics link nums: %s'%electronics_nums)
        # all
        print('all link: %s'%(fashion_nums + jewelry_nums + premium_nums + home_nums + electronics_nums))

    def count_data(self):
        # fashion
        fashion_nums = self.fashion_data.count()
        print('fashion data nums: %s'%fashion_nums)
        # jewelry
        jewelry_nums = self.jewelry_data.count()
        print('jewelry data nums: %s'%jewelry_nums)
        # premium
        premium_nums = self.premium_data.count()
        print('premium data nums: %s'%premium_nums)
        # home
        home_nums = self.home_data.count()
        print('home data nums: %s'%home_nums)
        # electronics
        electronics_nums = self.electronics_data.count()
        print('electronics data nums: %s'%electronics_nums)
        # all
        print('all data: %s'%(fashion_nums + jewelry_nums + premium_nums + home_nums + electronics_nums))

    def jewelry_data_insert(self):
        DBTables.insert(table_links=self.jewelry_links,table_data=self.jewelry_data,insert_type='jewelry')

    def fashion_data_insert(self):
        DBTables.insert(table_links=self.fashion_links,table_data=self.fashion_data,insert_type='fashion')

    def premium_data_insert(self):
        DBTables.insert(table_links=self.premium_links,table_data=self.premium_data,insert_type='premium')

    def home_data_insert(self):
        DBTables.insert(table_links=self.home_links,table_data=self.home_data,insert_type='home')

    def electronics_data_insert(self):
        DBTables.insert(table_links=self.electronics_links,table_data=self.electronics_data,insert_type='electronics')

    def all_data_insert(self):
        jewelry_thread = threading.Thread(target=self.jewelry_data_insert)
        jewelry_thread.start()
        fashion_thread = threading.Thread(target=self.fashion_data_insert)
        fashion_thread.start()
        premium_thread = threading.Thread(target=self.premium_data_insert)
        premium_thread.start()
        home_thread = threading.Thread(target=self.home_data_insert)
        home_thread.start()
        electronics_thread = threading.Thread(target=self.electronics_data_insert)
        electronics_thread.start()

    def jewelry_img_save(self,way):
        if way == 'urllib':
            self.save_img(table_data=self.jewelry_data,dirname='jewelry')
        elif way == 'requests':
            self.save_img_requests(table_data=self.jewelry_data,dirname='jewelry')

    def fashion_img_save(self,way):
        if way == 'urllib':
            self.save_img(table_data=self.fashion_data,dirname='fashion')
        elif way == 'requests':
            self.save_img_requests(table_data=self.fashion_data,dirname='fashion')

    def premium_img_save(self,way):
        if way == 'urllib':
            self.save_img(table_data=self.premium_data,dirname='premium')
        elif way == 'requests':
            self.save_img_requests(table_data=self.premium_data,dirname='premium')

    def home_img_save(self,way):
        if way == 'urllib':
            self.save_img(table_data=self.home_data,dirname='home')
        elif way == 'requests':
            self.save_img_requests(table_data=self.home_data,dirname='home')

    def electronics_img_save(self,way):
        if way == 'urllib':
            self.save_img(table_data=self.electronics_data,dirname='electronics')
        elif way == 'requests':
            self.save_img_requests(table_data=self.electronics_data,dirname='electronics')

    def all_img_save(self,way):
        jewelry_img_thread = threading.Thread(target=self.jewelry_img_save,args=(way,))
        fashion_img_thread = threading.Thread(target=self.fashion_img_save, args=(way,))
        premium_img_thread = threading.Thread(target=self.premium_img_save,args=(way,))
        home_img_thread = threading.Thread(target=self.home_img_save,args=(way,))
        electronics_img_thread = threading.Thread(target=self.electronics_img_save,args=(way,))
        jewelry_img_thread.start()
        fashion_img_thread.start()
        premium_img_thread.start()
        home_img_thread.start()
        electronics_img_thread.start()

    def save_img(self,table_data,dirname):
        path = os.path.join(IMG_PATH,dirname)
        if dirname not in os.listdir(IMG_PATH):
            os.mkdir(path)
        count = 0
        for item in table_data.find():
            count += 1
            data = item['data']
            main_image_url = data['main_image'].replace('thumbnail', 'large')
            #"https://images.tophatter.com/667a178828a83642a8d1a27c7251bff5/thumbnail.jpg",
            img_name = '{}.jpg'.format(main_image_url.split('/')[-2])
            if img_name not in os.listdir(path):
                img_save_path = os.path.join(path,img_name)
                try:
                    urlretrieve(main_image_url,img_save_path)
                    logging.info('{} dirname save {} success [{}]'.format(dirname,img_name,count))
                except:
                    logging.error('{} save {} failure [{}]'.format(dirname,img_name,count))
            else:
                logging.warning('{} dirname already exists {} [{}]'.format(dirname,img_name,count))

    def save_img_requests(self,table_data,dirname):
        path = os.path.join(IMG_PATH, dirname)
        if dirname not in os.listdir(IMG_PATH):
            os.mkdir(path)
        for count,item in enumerate(table_data.find(no_cursor_timeout = True)):
            if DBTables.is_exits_img(item=item,path=path,count=count,dirname=dirname):
                continue
            else:
                img_thread = threading.Thread(target=DBTables.img_requests_single,args=(path,item,count,dirname))
                img_thread.start()
                sleep(0.5)

    @staticmethod
    def is_exits_img(item,path,count,dirname):
        data = item['data']
        main_image_url = data['main_image']
        # "https://images.tophatter.com/667a178828a83642a8d1a27c7251bff5/thumbnail.jpg",
        img_name = '{}.jpg'.format(main_image_url.split('/')[-2])
        if img_name in os.listdir(path):
            logging.warning('{} dirname already exists {} [{}]'.format(dirname, img_name, count))
            return True
        else:
            return False

    @staticmethod
    def img_requests_single(path,item,count,dirname):
        headers = {
            'User_Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36',
            'cookie': 'source=lot-view-slot; visit_uuid=2b609307-69b4-4937-8fdc-420179b2b5c8; _gcl_au=1.1.391491282.1561910380; _ga=GA1.2.233243008.1561910380; _gid=GA1.2.2011755620.1561910380; cto_lwid=0ed33863-4306-4f8b-baaf-b341a40519be; _fbp=fb.1.1561910471016.1067669336; referrer=https%3A%2F%2Ftophatter.com%2F; visit_user_id=26548343; _auction_session=BAh7DEkiD3Nlc3Npb25faWQGOgZFVEkiJWJkNTYyNWY0ZDhmZWZiM2RmNjk1Y2MwMDViNTNkMDk4BjsAVEkiCWluaXQGOwBGVEkiEWluaXRpYXRlZF9hdAY7AEZJdToJVGltZQ3P1x2A2xmikgo6C29mZnNldGn%2BkJ06CXpvbmVJIghQRFQGOwBGOg1uYW5vX251bWkCygM6DW5hbm9fZGVuaQY6DXN1Ym1pY3JvIgaXSSIOcmV0dXJuX3RvBjsARkkiBi8GOwBGSSIQX2NzcmZfdG9rZW4GOwBGSSIxWTJCYkRvSkV5eG83UTJ0MmcwenpxMk55WGg1cS92UFRSZ2E2MklkOXJRRT0GOwBGSSIMdXNlcl9pZAY7AEZpBHcYlQFJIg9leHBpcmVzX2F0BjsARlU6IEFjdGl2ZVN1cHBvcnQ6OlRpbWVXaXRoWm9uZVsISXU7Bg3Q2R3AH5DHBAk7CEkiCFVUQwY7AEY7CWkCEQM7CmkGOwsiB3hQSSIfUGFjaWZpYyBUaW1lIChVUyAmIENhbmFkYSkGOwBUSXU7Bg3J2R3AH5DHBAk7CEkiCFVUQwY7AEY7CWkCEQM7CmkGOwsiB3hQ--290e0145dc7c460926f2412a428d7092f0e0a1a0; ab.storage.userId.b7f13edb-826b-483c-aaa5-db246d0be23e=%7B%22g%22%3A%2226548343%22%2C%22c%22%3A1561910483875%2C%22l%22%3A1561910483875%7D; ab.storage.deviceId.b7f13edb-826b-483c-aaa5-db246d0be23e=%7B%22g%22%3A%228eac8ffc-0503-7c67-2056-155761da5089%22%2C%22c%22%3A1561910483882%2C%22l%22%3A1561910483882%7D; ad_product_id=128572855; attendee_session_id=a58208c5-35f5-4152-beaa-dd9df9f56301; _gat=1; criteo_write_test=ChUIBBINbXlHb29nbGVSdGJJZBgBIAE; ab.storage.sessionId.b7f13edb-826b-483c-aaa5-db246d0be23e=%7B%22g%22%3A%225336ef66-461e-643b-eda9-1bf1494453c5%22%2C%22e%22%3A1561964407901%2C%22c%22%3A1561962589738%2C%22l%22%3A1561962607901%7D'
        }
        data = item['data']
        main_image_url = data['main_image'].replace('thumbnail', 'large')
        # "https://images.tophatter.com/667a178828a83642a8d1a27c7251bff5/thumbnail.jpg",
        img_name = '{}.jpg'.format(main_image_url.split('/')[-2])
        img_save_path = os.path.join(path, img_name)
        try:
            response = requests.get(url=main_image_url, headers=headers, timeout=60)
            img = response.content
            with open(img_save_path, 'wb') as f:
                f.write(img)
            logging.info('{} dirname save {} success [{}]'.format(dirname, img_name, count))
        except:
            logging.error('{} save {} failure [{}]'.format(dirname, img_name, count))

    @staticmethod
    def insert(table_links,table_data,insert_type):
        for count,item in enumerate(table_links.find(no_cursor_timeout = True)):
            if DBTables.check_insert_single(table_data=table_data,item=item,insert_type=insert_type,count=count):
                continue
            else:
                insert_single_thread = threading.Thread(target=DBTables.insert_single,args=(item,table_data,insert_type,count))
                insert_single_thread.start()
                sleep(1)

    @staticmethod
    def insert_single(item,table_data,insert_type,count):
        url = item['url']
        text = DBTables.get_text(url=url)
        if text is not None:
            end = text['bidding_ended_at']
            if end is not None:
                # print(end)
                logging.info('{} data table insert one info:{} [{}]'.format(insert_type, end, count))
                table_data.insert_one({'url': url, 'data': text})
            else:
                logging.debug('{} data bidding_ended_at is None [{}]'.format(insert_type, count))
        else:
            logging.error('{} type link request failure [{}]'.format(insert_type, count))

    @staticmethod
    def check_insert_single(table_data,item,insert_type,count):
        url = item['url']
        if table_data.find_one({'url':url}):
            # logging.warning('the information already exists in {} data table [{}]'.format(insert_type,count))
            return 1

    @staticmethod
    def get_text(url):
        try:
            headers = {
                'User_Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36',
                'cookie': 'source=lot-view-slot; visit_uuid=2b609307-69b4-4937-8fdc-420179b2b5c8; _gcl_au=1.1.391491282.1561910380; _ga=GA1.2.233243008.1561910380; _gid=GA1.2.2011755620.1561910380; cto_lwid=0ed33863-4306-4f8b-baaf-b341a40519be; _fbp=fb.1.1561910471016.1067669336; referrer=https%3A%2F%2Ftophatter.com%2F; visit_user_id=26548343; _auction_session=BAh7DEkiD3Nlc3Npb25faWQGOgZFVEkiJWJkNTYyNWY0ZDhmZWZiM2RmNjk1Y2MwMDViNTNkMDk4BjsAVEkiCWluaXQGOwBGVEkiEWluaXRpYXRlZF9hdAY7AEZJdToJVGltZQ3P1x2A2xmikgo6C29mZnNldGn%2BkJ06CXpvbmVJIghQRFQGOwBGOg1uYW5vX251bWkCygM6DW5hbm9fZGVuaQY6DXN1Ym1pY3JvIgaXSSIOcmV0dXJuX3RvBjsARkkiBi8GOwBGSSIQX2NzcmZfdG9rZW4GOwBGSSIxWTJCYkRvSkV5eG83UTJ0MmcwenpxMk55WGg1cS92UFRSZ2E2MklkOXJRRT0GOwBGSSIMdXNlcl9pZAY7AEZpBHcYlQFJIg9leHBpcmVzX2F0BjsARlU6IEFjdGl2ZVN1cHBvcnQ6OlRpbWVXaXRoWm9uZVsISXU7Bg3Q2R3AH5DHBAk7CEkiCFVUQwY7AEY7CWkCEQM7CmkGOwsiB3hQSSIfUGFjaWZpYyBUaW1lIChVUyAmIENhbmFkYSkGOwBUSXU7Bg3J2R3AH5DHBAk7CEkiCFVUQwY7AEY7CWkCEQM7CmkGOwsiB3hQ--290e0145dc7c460926f2412a428d7092f0e0a1a0; ab.storage.userId.b7f13edb-826b-483c-aaa5-db246d0be23e=%7B%22g%22%3A%2226548343%22%2C%22c%22%3A1561910483875%2C%22l%22%3A1561910483875%7D; ab.storage.deviceId.b7f13edb-826b-483c-aaa5-db246d0be23e=%7B%22g%22%3A%228eac8ffc-0503-7c67-2056-155761da5089%22%2C%22c%22%3A1561910483882%2C%22l%22%3A1561910483882%7D; ad_product_id=128572855; attendee_session_id=a58208c5-35f5-4152-beaa-dd9df9f56301; _gat=1; criteo_write_test=ChUIBBINbXlHb29nbGVSdGJJZBgBIAE; ab.storage.sessionId.b7f13edb-826b-483c-aaa5-db246d0be23e=%7B%22g%22%3A%225336ef66-461e-643b-eda9-1bf1494453c5%22%2C%22e%22%3A1561964407901%2C%22c%22%3A1561962589738%2C%22l%22%3A1561962607901%7D'
            }
            text = requests.get(url=url, headers=headers,timeout=600).text
            return json.loads(text)
        except(BaseException) as e:
            # print(url,e)
            return None

    def current_strftime(self):
        return datetime.now().strftime('%Y%m%d%H%M%S')

# 导出数据
class ExportData(DBTables):

    def __init__(self):
        super().__init__()

    def extract(self,table_data):
        data = []
        elements = ['_id','title','description','img_name','main_image','buy_now_price',
                'retail_price','starting_bid_amount','hammer_price',
                'shipping_price','seller_name','seller_lots_sold','ratings_count',
                'ratings_average','bidding_started_at','bidding_ended_at','bidding_time','link']
        for item in tqdm(table_data.find(),desc='load data'):
            try:
                one = item['data']
                # id
                _id = one['id']
                # 标题
                title = one['title']
                # 商品描述
                description = one['description']
                #####图片
                main_image = one['main_image'].replace('thumbnail','large')
                img_name = main_image.split('/')[-2]
                # 直接买的价格
                buy_now_price = one['buy_now_price']
                # 零售价
                retail_price = one['retail_price']
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
                bidding_started_at = ExportData.date_to_timestamp(one['bidding_started_at'])
                # 拍出时间
                bidding_ended_at = ExportData.date_to_timestamp(one['bidding_ended_at'])
                # 拍卖时长
                bidding_time = (bidding_ended_at - bidding_started_at).seconds
                # 链接
                link = item['url']
                data.append([_id,title,description,img_name,main_image,buy_now_price,
                    retail_price,starting_bid_amount,hammer_price,
                    shipping_price,seller_name,seller_lots_sold,ratings_count,
                    ratings_average,bidding_started_at,bidding_ended_at,bidding_time,link])
            except:
                pass
        return pd.DataFrame(data,columns=elements)

    def jewelry_data_export(self):
        self.save_csv(table_data=self.jewelry_data,file_type='jewelry-data')

    def fashion_data_export(self):
        self.save_csv(table_data=self.fashion_data,file_type='fashion-data')

    def premium_data_export(self):
        self.save_csv(table_data=self.premium_data,file_type='premium-data')

    def home_data_export(self):
        self.save_csv(table_data=self.home_data,file_type='home-data')

    def electronics_data_export(self):
        self.save_csv(table_data=self.electronics_data,file_type='electronics-data')

    def all_data_export(self):
        self.jewelry_data_export()
        self.fashion_data_export()
        self.premium_data_export()
        self.home_data_export()
        self.electronics_data_export()

    def save_csv(self,table_data,file_type):
        current_time = self.current_strftime()
        filename = '{}-{}.csv'.format(file_type,current_time)
        path = os.path.join(DATA_PATH,filename)
        df_data = self.extract(table_data=table_data)
        df_data.to_csv(path,index=False)

    @staticmethod
    def date_to_timestamp(date):
        return datetime.strptime(date.replace('T',' ').replace('+08:00',''),'%Y-%m-%d %H:%M:%S')

if __name__ == "__main__":

    '''
        导入数据与查询数据记录
    '''
    db = DBTables()
    print('------link------')
    db.count_link()
    print('\n------data------')
    db.count_data()
    # db.all_data_insert()
    while True:
        try:
            db.jewelry_data_insert()
            db.fashion_data_insert()
            db.premium_data_insert()
            db.home_data_insert()
            db.electronics_data_insert()
            # 多线程爬取
            db.all_data_insert()
        except(BaseException) as e:
            print('reconnect...')
            # print(e)
    # 图片爬取
    # db.jewelry_img_save(way='requests')
    # db.fashion_img_save(way='requests')
    # db.premium_img_save(way='requests')
    # db.home_img_save(way='requests')
    # db.electronics_img_save(way='requests')

    '''
        导出数据
    '''
    export = ExportData()
    # export.jewelry_data_export()
    # export.fashion_data_export()
    # export.premium_data_export()
    # export.home_data_export()
    # export.electronics_data_export()
    # export.count()
