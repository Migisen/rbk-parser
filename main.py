import numpy as np
import pandas as pd
import requests
import logging
import json
from multiprocessing import Pool

from bs4 import BeautifulSoup as bs


def request_rbk(url):
    try:
        rbk_page = requests.get(url)
        while rbk_page.status_code != 200:
            rbk_page = requests.get(url)
    except Exception as e:
        logging.error('Ошибка при получении старницы')
        raise
    return rbk_page


def parse_page(page):
    pool = Pool(12)
    rbk_soup = bs(page, features='html.parser')
    news_container = rbk_soup.find_all('div', {'class': 'item__wrap l-col-center'})
    result_news = []
    links = []
    for news in news_container:
        article_data = news.contents[3]
        news_date = str.replace(news.contents[1].text, '\n', '')
        news_link = article_data.get('href')
        links.append(news_link)
        news_name = str.replace(article_data.text, '\n', '').strip()
        result_news.append({'date': news_date, 'name': news_name, 'link': news_link})

    text_list, tag_list = zip(*pool.map(get_text, links))
    pool.close()
    pool.join()
    for text, tag, news in zip(text_list, tag_list, result_news):
        news['text'] = text
        news['tag'] = tag
    return result_news


def get_text(url):
    article_page = request_rbk(url)
    article_soup = bs(article_page.content, features='html.parser')
    text_container = article_soup.find_all('p')
    final_text = ''
    for text in text_container:
        if text.find(class_='article__inline-item') or text.find(class_='r-covid-19 js-covid'):
            continue
        final_text += text.text.rstrip()
    article_tag = article_soup.find('span', {'class', 'article__tags__block'}).text
    article_tag = str.replace(article_tag, '\n', '').strip()
    article_date = article_soup.find('span', {'class':'article__header__date'})

    return final_text, article_tag


def rbk_parser(url):
    rbk_request = request_rbk(url).content
    parse_page(rbk_request)

def api_parser(url):
    request_api = request_rbk(url)
    rbk_json = json.loads(request_api.text)
    json_soup = bs(rbk_json['html'], features='html.parser')
    page_content = json_soup.find_all('a')


if __name__ == '__main__':
    logging.basicConfig(filename='rbk-parser.log', level=logging.ERROR,
                        format='%(asctime)s:%(levelname)s:%(message)s')

    rbk_parser('https://www.rbc.ru/economics/')

