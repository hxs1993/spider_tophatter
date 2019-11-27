# _*_ coding:utf-8 _*_
__author__ = 'hxs'
__date__ = '2019/7/15 22:16'

'''
    多服务器数据迁移
'''

import pymongo
from tqdm import tqdm
from time import sleep
import threading
from config import MongoConnectDB

def other_to_local_for_data(other_host,local_host,skip_item):
    '''
    将外网数据库data数据导入本地
    :return:
    '''
    local_db = MongoConnectDB(host=local_host)
    other_db = MongoConnectDB(host=other_host)

    jewelry_count = 0
    for item in tqdm(other_db.jewelry_data.find({},{'data':1,'url':1,'_id':0},no_cursor_timeout = True).skip(skip_item['jewelry']),desc='jewelry data'):
        if local_db.jewelry_data.find_one({'url':item['url']}):
            continue
        else:
            local_db.jewelry_data.insert_one(item)
            jewelry_count += 1
    sleep(0.5)
    print('add jewelry data nums:%s'%jewelry_count)
    sleep(0.5)

    fashion_count = 0
    for item in tqdm(other_db.fashion_data.find({},{'data':1,'url':1,'_id':0},no_cursor_timeout = True).skip(skip_item['fashion']),desc='fashion data'):
        if local_db.fashion_data.find_one({'url':item['url']}):
            continue
        else:
            local_db.fashion_data.insert_one(item)
            fashion_count += 1
    sleep(0.5)
    print('add fashion data nums:%s'%fashion_count)
    sleep(0.5)

    premium_count = 0
    for item in tqdm(other_db.premium_data.find({},{'data':1,'url':1,'_id':0},no_cursor_timeout = True).skip(skip_item['premium']),desc='premium data'):
        if local_db.premium_data.find_one({'url':item['url']}):
            continue
        else:
            local_db.premium_data.insert_one(item)
            premium_count += 1
    sleep(0.5)
    print('add premium data nums:%s'%premium_count)
    sleep(0.5)

    home_count = 0
    for item in tqdm(other_db.home_data.find({},{'data':1,'url':1,'_id':0},no_cursor_timeout = True).skip(skip_item['home']),desc='home data'):
        if local_db.home_data.find_one({'url':item['url']}):
            continue
        else:
            local_db.home_data.insert_one(item)
            home_count += 1
    sleep(0.5)
    print('add home data nums:%s'%home_count)
    sleep(0.5)

    electronics_count = 0
    for item in tqdm(other_db.electronics_data.find({},{'data':1,'url':1,'_id':0},no_cursor_timeout = True).skip(skip_item['electronics']),desc='electronics data'):
        if local_db.electronics_data.find_one({'url':item['url']}):
            continue
        else:
            local_db.electronics_data.insert_one(item)
            electronics_count += 1
    sleep(0.5)
    print('add electronics data nums:%s'%electronics_count)
    sleep(0.5)

def other_to_local_for_links_thread(other_host,local_host,skip_item):
    local_db = MongoConnectDB(host=local_host)
    other_db = MongoConnectDB(host=other_host)
    def jewelry():
        jewelry_count = 0
        for item in tqdm(other_db.jewelry_links.find({}, {'url': 1, '_id': 0},no_cursor_timeout = True).skip(skip_item['jewelry']), desc='jewelry links'):
            if local_db.jewelry_links.find_one({'url': item['url']}):
                continue
            else:
                local_db.jewelry_links.insert_one(item)
                jewelry_count += 1
        sleep(0.5)
        print('add jewelry links nums:%s'%jewelry_count)
        sleep(0.5)

    def fashion():
        fashion_count = 0
        for item in tqdm(other_db.fashion_links.find({}, {'url': 1, '_id': 0},no_cursor_timeout = True).skip(skip_item['fashion']), desc='fashion links'):
            if local_db.fashion_links.find_one({'url': item['url']}):
                continue
            else:
                local_db.fashion_links.insert_one(item)
                fashion_count += 1
        sleep(0.5)
        print('add fashion links nums:%s'%fashion_count)
        sleep(0.5)

    def premium():
        premium_count = 0
        for item in tqdm(other_db.premium_links.find({}, {'url': 1, '_id': 0},no_cursor_timeout = True).skip(skip_item['premium']), desc='premium links'):
            if local_db.premium_links.find_one({'url': item['url']}):
                continue
            else:
                local_db.premium_links.insert_one(item)
                premium_count += 1
        sleep(0.5)
        print('add premium links nums:%s'%premium_count)
        sleep(0.5)

    def home():
        home_count = 0
        for item in tqdm(other_db.home_links.find({}, {'url': 1, '_id': 0},no_cursor_timeout = True).skip(skip_item['home']), desc='home links'):
            if local_db.home_links.find_one({'url': item['url']}):
                continue
            else:
                local_db.home_links.insert_one(item)
                home_count += 1
        sleep(0.5)
        print('add home links nums:%s'%home_count)
        sleep(0.5)

    def electronics():
        electronics_count = 0
        for item in tqdm(other_db.electronics_links.find({}, {'url': 1, '_id': 0},no_cursor_timeout = True).skip(skip_item['electronics']), desc='electronics links'):
            if local_db.electronics_links.find_one({'url': item['url']}):
                continue
            else:
                local_db.electronics_links.insert_one(item)
                electronics_count += 1
        sleep(0.5)
        print('add electronics links nums:%s'%electronics_count)
        sleep(0.5)

    jewelry_thread = threading.Thread(target=jewelry)
    fashion_thread = threading.Thread(target=fashion)
    premium_thread = threading.Thread(target=premium)
    home_thread = threading.Thread(target=home)
    electronics_thread = threading.Thread(target=electronics)
    jewelry_thread.start()
    fashion_thread.start()
    premium_thread.start()
    home_thread.start()
    electronics_thread.start()

def other_to_local_for_links(other_host,local_host,skip_item):
    local_db = MongoConnectDB(host=local_host)
    other_db = MongoConnectDB(host=other_host)

    jewelry_count = 0
    for item in tqdm(other_db.jewelry_links.find({}, {'url': 1, '_id': 0},no_cursor_timeout = True).skip(skip_item['jewelry']), desc='jewelry links'):
        if local_db.jewelry_links.find_one({'url': item['url']}):
            continue
        else:
            local_db.jewelry_links.insert_one(item)
            jewelry_count += 1
    sleep(0.5)
    print('add jewelry links nums:%s'%jewelry_count)
    sleep(0.5)

    fashion_count = 0
    for item in tqdm(other_db.fashion_links.find({}, {'url': 1, '_id': 0},no_cursor_timeout = True).skip(skip_item['fashion']), desc='fashion links'):
        if local_db.fashion_links.find_one({'url': item['url']}):
            continue
        else:
            local_db.fashion_links.insert_one(item)
            fashion_count += 1
    sleep(0.5)
    print('add fashion links nums:%s'%fashion_count)
    sleep(0.5)

    premium_count = 0
    for item in tqdm(other_db.premium_links.find({}, {'url': 1, '_id': 0},no_cursor_timeout = True).skip(skip_item['premium']), desc='premium links'):
        if local_db.premium_links.find_one({'url': item['url']}):
            continue
        else:
            local_db.premium_links.insert_one(item)
            premium_count += 1
    sleep(0.5)
    print('add premium links nums:%s'%premium_count)
    sleep(0.5)

    home_count = 0
    for item in tqdm(other_db.home_links.find({}, {'url': 1, '_id': 0},no_cursor_timeout = True).skip(skip_item['home']), desc='home links'):
        if local_db.home_links.find_one({'url': item['url']}):
            continue
        else:
            local_db.home_links.insert_one(item)
            home_count += 1
    sleep(0.5)
    print('add home links nums:%s'%home_count)
    sleep(0.5)

    electronics_count = 0
    for item in tqdm(other_db.electronics_links.find({}, {'url': 1, '_id': 0},no_cursor_timeout = True).skip(skip_item['electronics']), desc='electronics links'):
        if local_db.electronics_links.find_one({'url': item['url']}):
            continue
        else:
            local_db.electronics_links.insert_one(item)
            electronics_count += 1
    sleep(0.5)
    print('add electronics links nums:%s'%electronics_count)
    sleep(0.5)

if __name__ == '__main__':
    cloud_links_skip_item = {
        'jewelry':46163,
        'fashion':60322,
        'premium':20959,
        'home':59923,
        'electronics':43977,
    }
    cloud_data_skip_item = {
        'jewelry':0,
        'fashion':0,
        'premium':0,
        'home':0,
        'electronics':0,
    }
    n5110_data_skip_item = {
        'jewelry': 387762,
        'fashion': 513158,
        'premium': 219282,
        'home': 673873,
        'electronics': 644453,
    }
    n5110_link_skip_item = {
        'jewelry': 449046,
        'fashion': 568021,
        'premium': 240304,
        'home': 737458,
        'electronics': 792233,
    }
    local_host = 'localhost'
    other_host_g7 = '192.168.0.175'
    other_host_n5110 = '192.168.43.90'
    other_host_cloud = '108.61.204.4'
    # other_to_local_for_data(other_host=other_host_cloud,local_host=local_host,skip_item=cloud_data_skip_item)
    other_to_local_for_data(other_host=other_host_n5110,local_host=local_host,skip_item=n5110_data_skip_item)
    # other_to_local_for_links(other_host=other_host_cloud,local_host=local_host,skip_item=cloud_links_skip_item)
    other_to_local_for_links(other_host=other_host_n5110,local_host=local_host,skip_item=n5110_link_skip_item)
