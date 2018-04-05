import json
import logging
from worker import conn
from rq import Queue
from scraper import ModelSearch
import os
from pymongo import MongoClient

if os.environ.get('ENVIRONMENT') == 'production':
    logging.basicConfig(level=logging.INFO)
else:
    logging.basicConfig(level=logging.DEBUG)

q = Queue(connection=conn)

MONGODB_URI = os.environ.get('MONGODB_URI',
                             'mongodb://127.0.0.1:27017/used_cars')
db = MongoClient(MONGODB_URI).get_database()

if __name__ == '__main__':
    with open('config.json', 'r') as conf_file:
        config = json.load(conf_file)
        MODELS = [(car['brand'], car['model']) for car in config['models']]

    for brand, model in MODELS:
        search = ModelSearch(brand, model)
        for page in search.pages:
            q.enqueue(page.save_all(db['cars']))
