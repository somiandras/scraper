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


def encode_keys(data):
    keys = db['keys']
    for top_key in ['details', 'features', 'other']:
        old_keys = list(data[top_key].keys())
        for old_key in old_keys:
            saved_key = keys.find_one({'description': old_key})
            if saved_key is None:
                logging.debug('{} is not in keys collection'.format(old_key))
                count = keys.find({'type': top_key}).count()
                new_key = '{}{:0>3}'.format(top_key[:2].upper(), count + 1)
                logging.debug('Adding {} for {}'.format(new_key, old_key))
                keys.insert_one(
                    {'key': new_key, 'description': old_key, 'type': top_key})
            else:
                logging.debug('{} found in keys collection as {}'.format(
                    old_key, saved_key.get('key')))
                new_key = saved_key.get('key')
            value = data[top_key].pop(old_key)
            data[top_key][new_key] = value

    return data


def save_ad_data(ad):
    data = ad.data
    if data is not None:
        db_filter = {'url': data['url']}
        current_price = data['details'].get('Vételár (Ft)', None)
        if current_price is not None:
            update = {
                'price': current_price,
                'date': datetime.today().isoformat()
            }
        else:
            update = None
        db['cars'].update_one(db_filter,
                                {'$setOnInsert': encode_keys(data),
                                 '$set': {'last_updated': datetime.today().isoformat()},
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
