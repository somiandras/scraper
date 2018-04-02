#!env/bin python
#-*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import logging
from datetime import date
from calendar import monthrange
import re
from pymongo import MongoClient

client = MongoClient()
db = client['used_cars']


logging.basicConfig(level='DEBUG')

BASE_URL = 'https://www.hasznaltauto.hu/szemelyauto/'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
MODELS = [
    ('skoda', 'octavia'),
    ('opel', 'astra'),
    ('ford', 'focus'),
    ('volvo', 's40'),
    ('volvo', 'v40'),
    ()
]


def get_ad_links(brand, model):
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
    data['Title'] = soup.find('span', property='p:name').text

    data_table = soup.find('table', class_='hirdetesadatok')
    cells = data_table.find_all('td')

    for cell_index in range(0, len(cells), 2):
        key = cells[cell_index].text.strip()
        value = cells[cell_index + 1].text.strip()
        cleaned = clean_key_value(key, value)
        data['details'][cleaned[0]] = cleaned[1]

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

    numeric_fields = [
        'Csomagtartó',
        'Hengerűrtartalom',
        'Kilométeróra állása',
        'Saját tömeg',
        'Szállítható szem. száma',
        'Össztömeg',
        'Vételár',
        'Ár (EUR)'
    ]
    new_key = key[:-1]
    new_value = value

    if new_key in numeric_fields:
        value = re.sub('\xa0Ft', 'Ft', value)
        value = re.sub(r'€\s', '', value)
        value = value.replace(' ', '').replace('.', '')

        match = re.match(r'([0-9]*)(.*)?', value)
        if len(match.group(1)) > 0:
            new_value = int(match.group(1))
        else:
            new_value = ''
        if new_key != 'Ár (EUR)':
            new_key = new_key + ' ({})'.format(match.group(2))

    if new_key == 'Ajtók száma':
        new_value = int(value)

    if new_key == 'Teljesítmény':
        match = re.match(r'([0-9]{2,3})\skW,\s([0-9]{2,3})\sLE', value)
        new_value = int(match.group(2))
        new_key = new_key + ' (LE)'

    return (new_key, new_value)


def scrape(brand, model):
    '''
    Loop through ad links and save data to MongoDB collection
    '''

    filename = '{}_links.txt'.format(model)
    with open(filename, 'r') as links:
        for link in links:
            check = db.cars.find_one({'url': link.strip()})
            if check is None:
                logging.info('New link: {}'.format(link))
                try:
                    details = get_car_details(link)
                    details['Brand'] = brand
                    details['Model'] = model
                except Exception as e:
                    logging.error(link)
                    logging.error(e)
                else:
                    db.cars.update_one({'url': link.strip()}, {'$setOnInsert': details}, upsert=True)


if __name__ == '__main__':
    for brand, model in MODELS:
        logging.info('Starting {} {}'.format(brand, model))
        get_ad_links(brand, model)

    for brand, model in MODELS:
        logging.info('Scraping {} {}'.format(brand, model))
        scrape(brand, model)
