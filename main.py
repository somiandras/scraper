import json
import logging
from worker import conn
from rq import Queue
from scraper import ModelSearch
import os

if os.environ.get('ENVIRONMENT') == 'production':
    logging.basicConfig(level=logging.INFO)
else:
    logging.basicConfig(level=logging.DEBUG)

q = Queue(connection=conn)

if __name__ == '__main__':
    with open('config.json', 'r') as conf_file:
        config = json.load(conf_file)
        MODELS = [(car['brand'], car['model']) for car in config['models']]

    for brand, model in MODELS:
        search = ModelSearch(brand, model)
        for page in search.pages:
            q.enqueue(page.save_all)
