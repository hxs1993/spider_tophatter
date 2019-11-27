# _*_ coding:utf-8 _*_
__author__ = 'hxs'
__date__ = '2019/7/26 0:06'

from config import MongoConnectDB,DATA_PATH
import os
import datetime
from tqdm import tqdm
import pandas as pd
import numpy as np

'''
    从数据库导出近段时间排名靠前的商品信息
'''

def long_ago(days):
    current = datetime.datetime.now()
    history = current - datetime.timedelta(days=days)
    pipeline = [
        {
            '$match':{
                'bidding_started_at':{'$gt':history,'$lte':current}
            }
        }
    ]
    return pipeline


class HistoryData:

    def __init__(self):
        DB = MongoConnectDB()
        self.jewelry = DB.jewelry_attention
        self.fashion = DB.fashion_attention
        self.premium = DB.premium_attention
        self.home = DB.home_attention
        self.electronics = DB.electronics_attention

    def export(self,sign,days,nums):
        if sign == 'jewelry':
            jewelry_info = HistoryData.load_data(tb_name=self.jewelry,sign='jewelry',days=days)
            self.__top(data=jewelry_info,sign=sign,days=days,nums=nums)
            # return jewelry_info
        elif sign == 'fashion':
            fashion_info = HistoryData.load_data(tb_name=self.fashion,sign='fashion',days=days)
            self.__top(data=fashion_info,sign=sign,days=days,nums=nums)
            # return fashion_info
        elif sign == 'premium':
            premium_info = HistoryData.load_data(tb_name=self.premium,sign='premium',days=days)
            self.__top(data=premium_info,sign=sign,days=days,nums=nums)
            # return premium_info
        elif sign == 'home':
            home_info = HistoryData.load_data(tb_name=self.home,sign='home',days=days)
            self.__top(data=home_info,sign=sign,days=days,nums=nums)
            # return home_info
        elif sign == 'electronics':
            electronics_info = HistoryData.load_data(tb_name=self.electronics,sign='electronics',days=days)
            self.__top(data=electronics_info,sign=sign,days=days,nums=nums)
            # return electronics_info
        else:
            print('sign输入错误...')
            return 0

    def __top(self,data,sign,days,nums):
        # jewelry最近7天的top100数据
        print('开始数据导出...')
        s_time = datetime.datetime.now().strftime('%Y-%m-%d')
        filename = '{}最近{}天的top{}数据{}.csv'.format(sign,days,nums,s_time)
        SAVE_PATH = os.path.join(DATA_PATH,'top',filename)
        df_data = pd.DataFrame(data=data)
        sort_index = df_data['img_name'].value_counts().sort_values(ascending=False).index.values.tolist()
        # print(sort_index)
        headers = ['title','description', 'img_name', 'main_image', 'average_starting_bid_amount',
                   'average_hammer_price', 'average_shipping_price', 'seller_name', 'average_seller_lots_sold',
                   'average_ratings_count','average_ratings_average','product_lots_sold']
        store = []
        for name in sort_index[:nums]:
            d_title = df_data[df_data['img_name'] == name]['title'].values.tolist()
            title = d_title[-1]
            description = df_data[df_data['img_name'] == name]['description'].values.tolist()[-1]
            product_lots_sold = len(d_title)
            img_name = name
            main_image = df_data[df_data['img_name'] == name]['main_image'].values.tolist()[-1]
            seller_name = df_data[df_data['img_name'] == name]['seller_name'].values.tolist()[-1]

            average_starting_bid_amount = np.average(
                df_data[df_data['img_name'] == name]['starting_bid_amount'].astype('float').values)
            # print(average_starting_bid_amount)
            average_hammer_price = np.average(
                df_data[df_data['img_name'] == name]['hammer_price'].astype('float').values)
            average_seller_lots_sold = np.average(
                df_data[df_data['img_name'] == name]['seller_lots_sold'].astype('float').values)
            average_shipping_price = np.average(
                df_data[df_data['img_name'] == name]['shipping_price'].astype('float').values)
            average_ratings_count = np.average(
                df_data[df_data['img_name'] == name]['ratings_count'].astype('float').values)

            average_ratings_list = [rating for rating in df_data[df_data['img_name'] == name]['ratings_average'].values if rating > 0]
            if average_ratings_list:
                average_ratings_average = np.average(average_ratings_list)
            else:
                average_ratings_average = 0
            # print([rating for rating in df_data[df_data['img_name'] == name]['ratings_average'].values if rating > 0])
            store.append([
                title, description, img_name, main_image, average_starting_bid_amount, average_hammer_price,
                average_shipping_price,
                seller_name, average_seller_lots_sold,average_ratings_count,average_ratings_average,product_lots_sold
            ])
        if store:
            df_data_top = pd.DataFrame(store, columns=headers)
            df_data_top.to_csv(SAVE_PATH,index=False)
            print('%s导出成功...'%filename)
        else:
            print('%s导出失败...'%filename)

    @staticmethod
    def load_data(tb_name,sign,days):
        info = {
            'title':[],'description':[],'img_name':[],'main_image':[],'starting_bid_amount':[],'hammer_price':[],
            'shipping_price':[],'seller_name':[],'seller_lots_sold':[],'ratings_average':[],'ratings_count':[]
        }
        pipeline = long_ago(days=days)
        for item in tqdm(tb_name.aggregate(pipeline=pipeline),desc='load %s table data'%sign):
            title = item['title']
            info['title'].append(title)
            description = item['description']
            info['description'].append(description)
            img_name = item['img_name']
            info['img_name'].append(img_name)
            main_image = item['main_image']
            info['main_image'].append(main_image)
            starting_bid_amount = item['starting_bid_amount']
            info['starting_bid_amount'].append(starting_bid_amount)
            hammer_price = item['hammer_price']
            info['hammer_price'].append(hammer_price)
            shipping_price = item['shipping_price']
            info['shipping_price'].append(shipping_price)
            seller_name = item['seller_name']
            info['seller_name'].append(seller_name)
            seller_lots_sold = item['seller_lots_sold']
            info['seller_lots_sold'].append(seller_lots_sold)
            ratings_average = item['ratings_average']
            info['ratings_average'].append(ratings_average)
            ratings_count = item['ratings_count']
            info['ratings_count'].append(ratings_count)
        return info

if __name__ == '__main__':
    # long_ago(days=7)
    history = HistoryData()
    days = 7
    nums = 300
    history.export(sign='jewelry',days=days,nums=nums)
    history.export(sign='fashion',days=days,nums=nums)
    history.export(sign='premium', days=days, nums=nums)
    history.export(sign='home', days=days, nums=nums)
    history.export(sign='electronics', days=days, nums=nums)
