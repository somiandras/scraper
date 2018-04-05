#!usr/env/bin python
#-*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
from datetime import date
import re
import logging

class BasePage:
    '''
    Provide download method for handling webpages.
    '''

    BASE_URL = 'https://www.hasznaltauto.hu/szemelyauto/'
    HEADERS = {
        'User-Agent': ' '.join([
            'Mozilla/5.0',
            '(Macintosh; Intel Mac OS X 10_12_5)',
            'AppleWebKit/537.36 (KHTML, like Gecko)',
            'Chrome/59.0.3071.115 Safari/537.36'
        ])
    }

    def __init__(self, url):
        self.url = url

    def download(self):
        r = requests.get(self.url, headers=self.HEADERS)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.error(e)
        else:
            self.html = r.text
        
        return self


class AdPage(BasePage):
    '''
    Class for interacting with a single car ad page.
    '''

    def __init__(self, url, brand, model):
        self.url = url
        self.brand = brand
        self.model = model

    def parse(self):
        data = {}
        data['details'] = {}
        data['features'] = {}
        data['other'] = {}
        
        if self.html is None:
            raise Exception('No HTML to parse, call download() first')
        
        soup = BeautifulSoup(self.html, 'lxml')
        data['title'] = soup.find('div', class_='adatlap-cim').text.strip()

        data_table = soup.find('table', class_='hirdetesadatok')
        cells = data_table.find_all('td')

        for cell_index in range(0, len(cells), 2):
            key = cells[cell_index].text.strip()
            value = cells[cell_index + 1].text.strip()
            new_key, new_value = self._clean_key_value(key, value)
            data['details'][new_key] = new_value

        features = soup.find('div', class_='felszereltseg').find_all('li')

        for feature in features:
            data['features'][feature.text.strip()] = True

        description = soup.find(
            'div', class_='leiras').find('div').text.strip()
        data['description'] = description

        other_features = soup.find(
            'div', class_='egyebinformacio').find_all('li')
        
        for feature in other_features:
            data['other'][feature.text.strip()] = True

        data['brand'] = self.brand
        data['model'] = self.model
        data['scraped'] = date.today()

        self._data = data

        return self

    def _clean_key_value(self, key, value):
        '''
        Clean key-value pair
        '''

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
            if unit:
                # Add unit to key
                new_key = '{} ({})'.format(new_key, unit)
        else:
            new_value = new_value.replace('\xa0', ' ')

        return (new_key, new_value)

    @property
    def data(self):
        if self._data is None:
            self.parse()

        return self._data

    def save(self, collection):
        db_filter = {'url': self.url}
        status = {'status': 'active', 'last_updated': date.today()}
        update = {
            'price': self.data['details']['Vételár (Ft)'],
            'date': date.today()
        }
        collection.update_one(db_filter, 
                              {'$setOnInsert': self.data,
                               '$set': status,
                               '$push': {'updates': update}},
                              upsert=True)

class ResultsPage(BasePage):
    '''
    Class for interacting a single search results page.
    '''

    def __init__(self, url, brand, model):
        self.url = url
        self.brand = brand
        self.model = model

    def parse(self):
        if self.html is None:
            self.download()

        soup = BeautifulSoup(self.html, 'lxml')
        result_items = soup.find_all('div', class_='cim-kontener')
        self.links = [item.find('a').get('href') for item in result_items]
        return self

    @property
    def ads(self):
        '''
        Generator yielding AdPage instances for each ad link on the
        result page.
        '''

        if self.links is None:
            self.parse()

        for url in self.links:
            yield AdPage(url, self.brand, self.model)

    def save_all(self, collection):
        '''
        Save all ad pages on the given results list.
        '''

        for ad in self.ads:
            ad.save(collection)


class ModelSearch(BasePage):
    '''
    Search page for given brand and model.
    '''

    def __init__(self, brand, model):
        self.brand = brand
        self.model = model

    def parse(self):
        if self.html is None:
            self.download()
        try:
            soup = BeautifulSoup(self.html, 'lxml')
            last_page = int(soup.find('li', class_='last').text)
        except AttributeError as e:
            logging.error('Last page link not found on results page')
            logging.error(self.url)
            logging.error(e)
        else:
            self._page_count_cached = last_page

        return self

    @property
    def url(self):
        '''
        Compile the complete search url for the given brand and model.
        '''

        return '{0}/{1}/{2}'.format(self.BASE_URL, self.brand, self.model)

    @property
    def page_count(self):
        '''
        Download initial page of results list and parse the
        number of pages.
        '''

        if self._page_count_cached is None:
            self.parse()

        return self._page_count_cached

    @property
    def pages(self):
        '''
        Generator yielding result pages of the search for the given
        brand and model.
        '''

        for page in range(1, self.page_count + 1):
            url = '{}/page{}'.format(self.url, page)
            yield ResultsPage(url, self.brand, self.model)
