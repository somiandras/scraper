#!usr/env/bin python
#-*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import logging
from datetime import date
from calendar import monthrange
import re
from pymongo import MongoClient

db = MongoClient()['used_cars']

logging.basicConfig(level='DEBUG')

BASE_URL = 'https://www.hasznaltauto.hu/szemelyauto/'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
MODELS = [
    ('skoda', 'octavia'),
    ('opel', 'astra'),
    ('ford', 'focus'),
    ('volvo', 's40'),
    ('volvo', 'v40')
]


def save_ad_links(brand, model):
    '''
    Download initial page of results list, extract the number of result pages,
    loop through them and save all the individual ad links to a txt file
    '''
    
    # Initial page to get page numbers
    model_url = '{0}/{1}/{2}'.format(BASE_URL, brand, model)
    r = requests.get(model_url, headers=HEADERS)
    logging.debug('URL: {}, status: {}'.format(model_url, r.status_code))
    if r.status_code == requests.codes.ok:
        soup = BeautifulSoup(r.text, 'lxml')
        last_page_number = int(soup.find(title='Utolsó oldal').text)

        # Loop through results pages and get ad links
        for page in range(1, last_page_number + 1):
            url = model_url + '/page{}'.format(page)
            ad_page = requests.get(url, headers=HEADERS)

            if ad_page.status_code == requests.codes.ok:
                soup = BeautifulSoup(ad_page.text, 'lxml')
                result_items = soup.find_all('div', class_='talalati_lista_head')
                for result_item in result_items:
                    link = result_item.find('a').get('href')
                    db['links'].update_one({'url': link},
                                           {'$setOnInsert': {'url': link,
                                                             'brand': brand,
                                                             'model': model,
                                                             'inserted': date.today()
                                                            }},
                                           upsert=True)
            else:
                logging.error('Cannot get {}'.format(url))
    else:
        logging.error('Cannot get {}'.format(url))


def get_car_details(url):
    '''
    Extract car details from HTML and return data in dict
    '''

    data = {
        'details': {},
        'features': {},
        'other': {}
    }
    data['id'] = url.strip()[-8:]
    data['url'] = url.strip()
    r = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(r.text, 'lxml')
    data['title'] = soup.find('div', class_='adatlap-cim').text

    data_table = soup.find('table', class_='hirdetesadatok')
    cells = data_table.find_all('td')

    for cell_index in range(0, len(cells), 2):
        key = cells[cell_index].text.strip()
        value = cells[cell_index + 1].text.strip()
        new_key, new_value = clean_key_value(key, value)
        data['details'][new_key] = new_value

    features = soup.find('div', class_='felszereltseg').find_all('li')
    for feature in features:
        data['features'][feature.text.strip()] = True

    description = soup.find('div', class_='leiras').find('div').text.strip()
    data['description'] = description

    other_features = soup.find('div', class_='egyebinformacio').find_all('li')
    for feature in other_features:
        data['other'][feature.text.strip()] = True

    return data


def clean_key_value(key, value):
    '''
    Clean key-value pair
    '''

    # Strip ":" from end of key
    new_key = key[:-1]
    new_value = value

    # Try to extract numerical values
    value_match = re.match(r'''
        ^[^/]*?
        [\s\xa0]?
        ((?:[\s\xa0]?[0-9]{1,3})+)
        [\s\xa0]?
        ([^0-9\s\xa0]*)$
    ''', value, re.VERBOSE)

    if value_match:
        # Numerical value found, update key and value
        extracted_value = value_match.group(1).strip()
        new_value = int(re.sub(r'\s|\xa0', '', extracted_value))
        unit = value_match.group(2)
        new_key = '{} ({})'.format(new_key, unit)

    return (new_key, new_value)


def scrape_ads():
    '''
    Loop through ad links and save data to MongoDB collection
    '''

    links = db['links'].find({'done': {'$exists': False}}, projection=['url'])
    for link in links:
        check = db['cars'].find_one({'url': link['url']})
        if check is None:
            logging.info('New link: {}'.format(link))
            try:
                details = get_car_details(link)
                details['brand'] = brand
                details['model'] = model
                details['scraped'] = date.today()
            except Exception as e:
                logging.error(e)
                logging.error(link)
            else:
                db['cars'].update_one({'url': link['url']},
                                      {'$setOnInsert': details},
                                      upsert=True)
        db['links'].update_one({'url': link['url']}, {'$set': {'done': True}})


if __name__ == '__main__':
    for brand, model in MODELS:
        logging.info('Starting {} {}'.format(brand, model))
        save_ad_links(brand, model)

    scrape_ads()
