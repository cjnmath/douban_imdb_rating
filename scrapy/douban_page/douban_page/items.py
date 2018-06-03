# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class DoubanPageItem(Item):
	# define the fields for your item here like:
		pass


class DoubanIndex(Item):
	# index json
	movie_douban_url = Field()
	movie_name_cn = Field()
	movie_region_id = Field()
	movie_cast_cn = Field()
	movie_director_cn = Field()
	movie_douban_score = Field()


class DoubanPage(Item):
	# douban page
	movie_douban_url = Field()
	movie_imdb_url = Field()
