import json
import logging
from worker import conn
from rq import Queue
from scraper import get_ad_links

q = Queue(connection=conn)

if __name__ == '__main__':
    with open('config.json', 'r') as conf_file:
        config = json.load(conf_file)
        MODELS = [(car['brand'], car['model']) for car in config['models']]

    for brand, model in MODELS:
        q.enqueue(get_ad_links, brand, model)
