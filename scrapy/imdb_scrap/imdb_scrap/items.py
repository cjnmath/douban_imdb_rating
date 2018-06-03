# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
from scrapy import Item, Field


class ImdbScrapItem(Item):
    # url for indentify
    url = Field()

    # people Field
    director = Field()
    writer = Field()
    starts = Field()
    cast = Field()

    # description
    imdb_score = Field()
    description = Field()
    keyword = Field()
    genre = Field()
    country = Field()
    language = Field()
    length = Field()
    budget = Field()
    Worldwide_Gross = Field()
