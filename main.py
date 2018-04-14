#!/usr/bin/env python
# -*- coding: utf8 -*-

import os
import sys
import json
import logging
from datetime import datetime
from pymongo import MongoClient
import scraper

MONGODB_URI = os.environ.get('MONGODB_URI', 'mongodb://127.0.0.1:27017/used_cars')
db = MongoClient(MONGODB_URI).get_database()


def encode_keys(data):
    '''
    Replace long feature definitions with standardized labels from keys 
    collection. If the definition is not in the keys collection, generate
    new label and save it to the collection.

    Params:  
    `data`: ad data object to replace keys in  

    Returns: `data` object with replaced keys
    '''

    keys = db['keys']
    for top_key in ['details', 'features', 'other']:
        old_keys = list(data[top_key].keys())
        for old_key in old_keys:
            saved_key = keys.find_one({'description': old_key})
            if saved_key is None:
                count = keys.find({'type': top_key}).count()
                new_key = '{}{:0>3}'.format(top_key[:2].upper(), count + 1)
                logger.debug('Adding {} for {}'.format(new_key, old_key))
                keys.insert_one(
                    {'key': new_key, 'description': old_key, 'type': top_key})
            else:
                new_key = saved_key.get('key')
            value = data[top_key].pop(old_key)
            data[top_key][new_key] = value

    return data


def process(obj):
    '''
    Process page object according to its type: iterate through results
    pages of a search page, iterate through ad pages of a results page or
    save data from an ad page, if the data is not already in the database.

    Params:  
    `obj`: page object with type of `scraper.ModelSearch`,
    `scraper.ResultsPage` or `scraper.AdPage`.  

    Returns: boolean: whether the processing was successful (`True`) or 
    resulted in error (`False`)
    '''

    if isinstance(obj, scraper.ModelSearch):
        for page in obj.pages:
            process(page)
    
    elif isinstance(obj, scraper.ResultsPage):
        for ad in obj.ads:
            found = db['cars'].find_one({'url': ad.url})
            if found is None:
                process(ad)
            else:
                logger.debug('Skipping {}'.format(ad.url))
                db['cars'].update_one(
                    {'url': ad.url},
                    {'$set': {'last_updated': datetime.today().isoformat()}}
                )

    elif isinstance(obj, scraper.AdPage):
        if obj.data is not None:
            db['cars'].update_one({'url': obj.data['url']},
                                  {'$setOnInsert': encode_keys(obj.data),
                                   '$set':
                                       {'last_updated': datetime.today().isoformat()}},
                                  upsert=True)
    else:
        raise Exception('Illegal type: {}'.format(type(obj).__name__))

    if obj.status and obj.status != 200:
        # Call resulted in non-OK code, add/update error entry
        db['errors'].update_one({'url': obj.url}, 
                                {'$setOnInsert': {'url': obj.url,
                                                  'brand': obj.brand,
                                                  'model': obj.model,
                                                  'type': type(obj).__name__},
                                 '$inc': {'attempts': 1},
                                 '$set': {'last_occured': datetime.today().isoformat(),
                                          'last_status': obj.status}
                                }, upsert=True)
        return False
    else:
        # All good, processing seems to be successful
        logger.info('Processed {}'.format(obj.url))
        return True


def retry_errors():
    '''
    Retry downloading pages in errors collection with 5xx statuses.

    Params: `None`  
    Returns: `None`
    '''

    errors = db['errors'].find(
        {'status': {'$gte': 500, '$lt': 600}, 'attempts': {'$lt': 2}})

    if errors.count() > 0:
        logger.info('{} errors found, retrying...'.format(errors.count()))
        count = 0
        for error in errors:
            # Reconstruct the object that resulted in the error
            class_ = getattr(scraper, error['type'])
            obj = class_(error['url'], error['brand'], error['model'])

            # Process it
            result = process(obj)
            if result:
                # Successfully fixed error, remove from collection
                db['errors'].delete_one({'_id': error['_id']})
            count += result
        
        logger.info('{} errors corrected'.format(count))
        
        # Iterate on possible new errors
        retry_errors()
    else:
        logger.info('All fixable errors are corrected')
        return True


if __name__ == '__main__':
    # Set up logger and console handler with formatting
    logger = logging.getLogger()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Check for debugging option in arguments
    if '--debug' in sys.argv:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Check for --log and filename option in arguments
    try:
        index = sys.argv.index('--log')
    except Exception:
        pass
    else:
        # Add file handler to logging with the same level and formatter
        filename = sys.argv[index + 1]
        fh = logging.FileHandler(filename, mode='w')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # Empty error collection
    db['errors'].delete_many({})

    # Load brands and models from external config
    with open('config.json', 'r') as conf_file:
        config = json.load(conf_file)
        MODELS = [(car['brand'], car['model']) for car in config['models']]

    # Initiate search for the given brand/model pairs
    for brand, model in MODELS:
        search = scraper.ModelSearch(None, brand, model)
        process(search)

    # Retry 5xx errors
    retry_errors()

    logging.info('Finished for now, exiting...')
    sys.exit(0)
