# -*- coding: utf-8 -*-
import scrapy
from scrapy.loader import ItemLoader
from douban_page.items import DoubanPage
import pymongo


class DoubanPageSpider(scrapy.Spider):
    name = 'douban_page_mongo'
    allowed_domains = ['movie.douban.com']
    # start_urls = ['https://www.douban.com/accounts/login?source=movie']

    def start_requests(self):
        login_url = 'https://www.douban.com/accounts/login?source=movie'
        yield scrapy.FormRequest(
                                url=login_url,
                                formdata={
                                        'form_email': 'your_username',
                                        'form_password': 'your_passwd'},
                                callback=self.login)

    def parse(self, response):
            # if response.status == 404:
            #     item = DoubanPage()
            #     item['movie_douban_url'] = [response.url]
            #     item['movie_imdb_url'] = [str(404)]
            #     return item
        loader = ItemLoader(item=DoubanPage(), response=response)
        try:
            check_imdb_url = response.xpath('//div[@id="info"]//a/@href').extract()[-1]
            if check_imdb_url.startswith(r'http://www.imdb.com/title/'):
                imdb_url = check_imdb_url
        except Exception as e:
            imdb_url = ''
        loader.add_value('movie_imdb_url', imdb_url)
        loader.add_value('movie_douban_url', response.url)
        return loader.load_item()

    def login(self, response):
        self.db = pymongo.MongoClient('mongodb://localhost:27017/')['douban_imdb_rating']
        for doc in self.db.all_data.find({"movie_imdb_url": {"$exists": False}}).limit(500):
            yield scrapy.Request(url=doc['movie_douban_url'], callback=self.parse)
