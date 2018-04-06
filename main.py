#!/usr/bin/env python
# -*- coding: utf8 -*-

import os
import json
import logging
from datetime import date
from scraper import ModelSearch
from pymongo import MongoClient

MONGODB_URI = os.environ.get('MONGODB_URI', 'mongodb://127.0.0.1:27017/used_cars')
db = MongoClient(MONGODB_URI).get_database()

logging.basicConfig(level=logging.DEBUG)


def save_ad_data(ad):
    data = ad.data
    db_filter = {'url': data['url']}
    status = {'status': 'active', 'last_updated': date.today().isoformat()}
    update = {
        'price': data['details'].get('Vételár (Ft)', None),
        'date': date.today().isoformat()
    }
    db['cars'].update_one(db_filter,
                               {'$setOnInsert': data,
                                '$set': status,
                                '$push': {'updates': update}},
                               upsert=True)

if __name__ == '__main__':
    with open('config.json', 'r') as conf_file:
        config = json.load(conf_file)
        MODELS = [(car['brand'], car['model']) for car in config['models']]

    for brand, model in MODELS:
        search = ModelSearch(brand, model)
        for page in search.pages:
            for ad in page.ads:
                save_ad_data(ad)
