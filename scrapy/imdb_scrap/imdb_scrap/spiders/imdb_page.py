# -*- coding: utf-8 -*-
import scrapy
# from scrapy.crawler.Crawler import settings
from scrapy.loader import ItemLoader
from imdb_scrap.items import ImdbScrapItem
import pymongo
import re


class ImdbPageSpider(scrapy.Spider):
    name = 'imdb_page_mongo'
    allowed_domains = ['www.imdb.com']

    def start_requests(self):
        client = pymongo.MongoClient()
        db = client[self.settings.get('MONGO_DATABASE')]
        for doc in db.all_data.find(
                        {'$and': [
                            {"movie_imdb_url": {"$exists": True}},
                            {"imdb_score": {"$exists": False}}]}).limit(1000):
            # ensure that the movie_imdb_url is not empty
            if not len(doc['movie_imdb_url']) == 0:
                yield scrapy.Request(
                                url=doc['movie_imdb_url'],
                                callback=self.parse)

    def parse(self, response):
        loader = ItemLoader(item=ImdbScrapItem(), response=response)

        def get_original_url():
            return re.sub('https', 'http', response.url)[:-1]

        loader.add_value('url', get_original_url())
        # people
        loader.add_xpath(
            'director',
            '//span[@itemprop="director"]//span[@itemprop="name"]/text()')
        loader.add_xpath(
            'writer',
            '//span[@itemprop="creator"]//span[@itemprop="name"]/text()')
        loader.add_xpath(
            'starts',
            '//span[@itemprop="actors"]//span[@itemprop="name"]/text()')
        loader.add_xpath(
            'cast',
            '//table[@class="cast_list"]//span[@itemprop="name"]/text()')

        def clean_empty(v):
            r = []
            for i in v:
                c = i.strip()
                if len(c) != 0:
                    r.append(c)
            return r

        # description
        loader.add_xpath(
            'imdb_score',
            '//span[@itemprop="ratingValue"]/text()')
        loader.add_xpath(
            'description',
            '//div[@itemprop="description"]/p/text()',
            clean_empty)
        loader.add_xpath(
            'keyword',
            '//div[@itemprop="keywords"]//span[@itemprop="keywords"]/text()')
        loader.add_xpath(
            'genre',
            '//div[@itemprop="genre"]//a/text()')
        loader.add_xpath(
            'country',
            '//div[@id="titleDetails"]//div[@class="txt-block"]\
            //h4[contains(text(), "Country:")]/../a/text()')
        loader.add_xpath(
            'language',
            '//div[@id="titleDetails"]//div[@class="txt-block"]\
            //h4[contains(text(), "Language:")]/../a/text()')
        loader.add_xpath(
            'length',
            '//div[@id="titleDetails"]//time[@itemprop="duration"]/text()')
        loader.add_xpath(
            'budget',
            '//div[@id="titleDetails"]//div[@class="txt-block"]\
            //h4[contains(text(), "Budget")]/../text()',
            clean_empty)
        loader.add_xpath(
            'Worldwide_Gross',
            '//div[@id="titleDetails"]//div[@class="txt-block"]\
            //h4[contains(text(), "Cumulative Worldwide Gross:")]/../text()',
            clean_empty)
        # url = response.url[:-1]  # remove '/'
        # cnx = make_mysql_connection(**mysql_config)
        # delete_row_by_url(
        #     connect_point=cnx,
        #     table_name='douban_to_imdb',
        #     target_url=url)
        return loader.load_item()
