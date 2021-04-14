import logging
import sqlite3
import json
from typing import List

import requests
from bs4 import BeautifulSoup as bs
from requests.api import request


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('parser.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


class RBKParser:
    def __init__(self, tag: str, db_connection: sqlite3.Connection) -> None:
        self.__base_url = 'https://www.rbc.ru/v10/search/ajax/'
        self.__limit = 100
        self.__offset = 0
        self.__con = db_connection
        self.tag = tag

    def commit_to_db(self, data: List[dict]) -> None:
        """Commit parsed data to sqlite db
        Args:
            data (List[dict]): data to commit
        """
        with self.__con:
            for article in data:
                self.__con.execute(
                    """
                        insert into News (date, title, url, content)
                        values(:title, :date, :url, :text)
                    """, article)

    def start_parsing(self):
        feed_page = self.get_page()
        while len(feed_page['items']) != 0:
            articles = []
            for item in feed_page['items']:
                article_url = item['fronturl']
                article_date = item['publish_date']
                article_title = item['title']
                article_text = self.get_text(article_url)
                articles.append({'title': article_title, 'date': article_date, 'url': article_url, 'text': article_text})
            self.commit_to_db(articles)
            logger.info(f'{self.__offset} articles parsed')
            self.__offset = self.__offset + self.__limit
            feed_page = self.get_page()

    def get_text(self, url):
        article_page = self.get_response(url, None)
        article_soup = bs(article_page, features='lxml')
        text_container = article_soup.find_all('p')
        final_text = ''
        for text in text_container:
            if text.find(class_='article__inline-item') or text.find(class_='r-covid-19 js-covid'):
                continue
            final_text += text.text.rstrip()
        return final_text

    def get_page(self):
        request_params = {'limit': self.__limit, 'offset': self.__offset, 'tag': self.tag}
        page_response = self.get_response(self.__base_url, request_params)
        page_json = json.loads(page_response)
        return page_json

    @staticmethod
    def get_response(url: str, request_params: dict, important: bool = False) -> str:
        """Tries to request provided url
        Args:
            url (str): website url
            request_params (dict): parameters of limit and offset
            important (bool, optional): rise exception if bad request. Defaults to False.
        Raises:
            Exception: Could not parse important page
        Returns:
            str: page text if 200 response, empty string otherwise
        """
        response = requests.get(url, request_params)
        try:
            assert response.status_code == 200, f'Could not parse provided url: {url}'
            return response.text
        except AssertionError as e:
            logger.error(str(e))
            if important:
                logger.error('Could not parse important page')
                raise Exception('Could not parse important page')
            return response.text
