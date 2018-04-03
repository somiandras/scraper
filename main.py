import json
import logging
from worker import conn
from rq import Queue
from scraper import get_ad_links, update_ad_data, db

q = Queue(connection=conn)

if __name__ == '__main__':
    with open('config.json', 'r') as conf_file:
        config = json.load(conf_file)
        MODELS = [(car['brand'], car['model']) for car in config['models']]

    for brand, model in MODELS:
        get_ad_links(brand, model)

    links = [(l['url'], l['brand'], l['model']) for l in db['links'].find()]
    for url, brand, model in links:
        q.enqueue(update_ad_data, url, brand, model)
        db['links'].delete_one({'url': url})
