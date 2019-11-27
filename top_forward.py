
# _*_ coding:utf-8 _*_
__author__ = 'hxs'
__date__ = '2019/7/6 18:33'

'''
    导出前XXX的数据,在执行导出操作前需在download中export对应的文件
'''

import pandas as pd
from download import DATA_PATH
import os
import numpy as np

DOC_PATH = os.path.join(DATA_PATH,'fashion-data-20190721171047.csv')

df_data = pd.read_csv(DOC_PATH)
sort_index = df_data['img_name'].value_counts().sort_values(ascending=False).index.values.tolist()
# print(sort_index)
headers = ['title','description','img_name','main_image','average_starting_bid_amount','average_hammer_price','average_shipping_price','seller_name','average_seller_lots_sold','average_ratings_count','product_lots_sold']
store = []
for name in sort_index[:100]:
    d_title = df_data[df_data['img_name'] == name]['title'].values.tolist()
    title = d_title[-1]
    description = df_data[df_data['img_name'] == name]['description'].values.tolist()[-1]
    product_lots_sold = len(d_title)
    img_name = name
    main_image = df_data[df_data['img_name'] == name]['main_image'].values.tolist()[-1]
    seller_name = df_data[df_data['img_name'] == name]['seller_name'].values.tolist()[-1]

    average_starting_bid_amount  = np.average(
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
    store.append([
        title,description,img_name,main_image,average_starting_bid_amount,average_hammer_price, average_shipping_price,
        seller_name, average_seller_lots_sold, average_ratings_count,product_lots_sold
    ])

df_data_top100 = pd.DataFrame(store,columns=headers)
df_data_top100.to_csv('fashion_top100.csv',index=False)
