# -*- coding: utf-8 -*-
import scrapy
import json
import re
from douban_page.items import DoubanIndex
from scrapy.loader import ItemLoader


class DoubanIndexSpider(scrapy.Spider):
    name = 'douban_index'
    allowed_domains = ['movie.douban.com']

    def start_requests(self):
        URL = 'https://movie.douban.com/j/new_search_subjects?sort=T&range=0,10&tags=电影,{}&start=0'
        REGIONS = {	'CN': '大陆', 'US': '美国', 'HK': '香港', 'TW': '台湾',
                    'JP': '日本', 'KR': '韩国', 'UK': '英国', 'FR': '法国',
                    'DE': '德国', 'IT': '意大利', 'ES': '西班牙', 'IN': '印度',
                    'TH': '泰国', 'RU': '俄罗斯', 'IR': '伊朗', 'CA': '加拿大',
                    'AU': '澳大利亚', 'IE': '爱尔兰', 'SE': '瑞典', 'BR': '巴西',
                    'DK': '丹麦'}

        yield scrapy.Request(	url=URL.format(REGIONS[self.region_id]),
                                # use "-a region_id='CN' " to pass CN.
                                callback=self.parse)

    def parse(self, response):
		# subtitude page number with "{}"
        URL = re.sub(r'(?<=start=)\d+', r'{}', response.url)
        json_response = json.loads(response.text)
        page_num = int(re.search(r'(?<=start=)\d+', response.url).group())
        if len(json_response['data']) != 0:
            page_num += 20
            yield scrapy.Request(URL.format(str(page_num)), callback=self.parse)
            # load content
            for i in range(len(json_response['data'])):
                loader = ItemLoader(item=DoubanIndex(), response=response)
                movie_douban_url = json_response['data'][i]['url']
                loader.add_value('movie_douban_url', movie_douban_url)
                loader.add_value('movie_region_id', self.region_id)
                loader.add_value('movie_cast_cn', json_response['data'][i]['casts'])
                loader.add_value('movie_name_cn', json_response['data'][i]['title'])
                loader.add_value('movie_director_cn', json_response['data'][i]['directors'])
                loader.add_value('movie_douban_score', json_response['data'][i]['rate'])
                yield loader.load_item()
