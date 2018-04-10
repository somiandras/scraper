#!/usr/bin/env python
# -*- coding: utf8 -*-

import os
import sys
import json
import logging
from datetime import datetime
from scraper import ModelSearch
from pymongo import MongoClient

MONGODB_URI = os.environ.get('MONGODB_URI', 'mongodb://127.0.0.1:27017/used_cars')
db = MongoClient(MONGODB_URI).get_database()


def save_ad_data(ad):
    data = ad.data
    if data is not None:
        db_filter = {'url': data['url']}
        status = {'status': 'active', 'last_updated': datetime.today().isoformat()}
        current_price = [f['value'] for f in data['features'] if f['key'] == 'Vételár (Ft)']
        if len(current_price) > 0:
            update = {
                'price': current_price[0],
                'date': datetime.today().isoformat()
            }
        else:
            update = None
        db['cars'].update_one(db_filter,
                                {'$setOnInsert': data,
                                    '$set': status,
                                    '$push': {'updates': update}},
                                upsert=True)

if __name__ == '__main__':
    if '--debug' in sys.argv:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    with open('config.json', 'r') as conf_file:
        config = json.load(conf_file)
        MODELS = [(car['brand'], car['model']) for car in config['models']]

    for brand, model in MODELS:
        search = ModelSearch(brand, model)
        for page in search.pages:
            for ad in page.ads:
                save_ad_data(ad)
