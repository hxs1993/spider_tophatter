# _*_ coding:utf-8 _*_
__author__ = 'hxs'
__date__ = '2019/7/25 1:01'

import pymongo
import os
import logging


logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_PATH,'data')

class MongoConnectDB:
    # fashion
    fashion = None
    fashion_links = None
    fashion_data = None
    fashion_attention = None
    # jewelry
    jewelry = None
    jewelry_links = None
    jewelry_data = None
    jewelry_attention = None
    # premium
    premium_products = None
    premium_links = None
    premium_data = None
    premium_attention = None
    # home
    home = None
    home_links = None
    home_data = None
    home_attention = None
    # electronics
    electronics = None
    electronics_links = None
    electronics_data = None
    electronics_attention = None

    def __init__(self,host='localhost'):
        client = pymongo.MongoClient(host,27017)
        # fashion
        self.fashion = client['fashion']
        self.fashion_links = self.fashion['good_links']
        self.fashion_data = self.fashion['good_data']
        self.fashion_attention = self.fashion['attention']
        # jewelry
        self.jewelry = client['jewelry']
        self.jewelry_links = self.jewelry['good_links']
        self.jewelry_data = self.jewelry['good_data']
        self.jewelry_attention = self.jewelry['attention']
        # premium
        self.premium_products = client['premium_products']
        self.premium_links = self.premium_products['good_links']
        self.premium_data = self.premium_products['good_data']
        self.premium_attention = self.premium_products['attention']
        # home
        self.home = client['home']
        self.home_links = self.home['good_links']
        self.home_data = self.home['good_data']
        self.home_attention = self.home['attention']
        # electronics
        self.electronics = client['electronics']
        self.electronics_links = self.electronics['good_links']
        self.electronics_data = self.electronics['good_data']
        self.electronics_attention = self.electronics['attention']