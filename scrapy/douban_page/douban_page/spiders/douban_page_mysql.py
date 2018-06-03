# -*- coding: utf-8 -*-
import scrapy
from scrapy.loader import ItemLoader
from douban_page.items import DoubanPage
import mysql.connector


class DoubanPageSpider(scrapy.Spider):
    name = 'douban_page_mysql'
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
        loader = ItemLoader(item=DoubanPage(), response=response)
        imdb_url = ''
        check_imdb_url = response.xpath('//div[@id="info"]\
                                            //a/@href').extract()[-1]
        if check_imdb_url.startswith(r'http://www.imdb.com/title/'):
            imdb_url = check_imdb_url
        loader.add_value('movie_imdb_url', imdb_url)
        loader.add_value('movie_douban_url', response.url)
        # change in database table , double entry recore
        cnx = make_mysql_connection(**config)
        processing_url = response.url
        save_row_by_url(
                        connect_point=cnx,
                        source_table_name='unduplicated_douban_index',
                        target_url=processing_url,
                        target_table_name='scraped_douban_index')
        delete_row_by_url(
                        connect_point=cnx,
                        table_name='unduplicated_douban_index',
                        target_url=processing_url)
        cnx.commit()
        cnx.close()
        return loader.load_item()

    def login(self, response):
        cnx = make_mysql_connection(**config)
        url_query_cursor = url_query(
                                    connect_point=cnx,
                                    table_name='unduplicated_douban_index',
                                    limit_num=self.limit_num)
        for (
                movie_name_cn,
                movie_douban_url,
                movie_region_id,
                movie_douban_score) in url_query_cursor:
            yield scrapy.Request(url=movie_douban_url, callback=self.parse)
        cnx.close()


###############################################################################
config = {
    'user': 'jc',
    'password': 'aller',
    'host': '127.0.0.1',
    'port': 3306,
    'database': 'douban_imdb_rating',
    'raise_on_warnings': True
    }


def make_mysql_connection(**kwargs):
    """ return a mysql connecting point
    """
    return mysql.connector.connect(**kwargs)


def url_query(connect_point=None, table_name='', limit_num=500):
    """ return a cursor (mysql.connector.connect.cursor)
        if connect_point (mysql.connector.connect)
        and table_name (str) is given
    """
    if not connect_point:
        print('Missing connection')
    elif not table_name:
        print('Missing table')
    else:
        cursor = connect_point.cursor(buffered=True)
        url_query = """
        SELECT movie_name_cn, movie_douban_url,
        movie_region_id, movie_douban_score
        FROM {} LIMIT {};
        """.format(table_name, str(limit_num))
        cursor.execute(url_query)
        return cursor


def save_row_by_url(connect_point=None, source_table_name='',
                    target_table_name='', target_url=''):
    if not connect_point:
        print('Missing connection')
    elif not source_table_name:
        print('Missing source table')
    elif not target_table_name:
        print('Missing target table')
    elif not target_url:
        print('Missing url')
    else:
        cursor = connect_point.cursor(buffered=True)
        save_row_query = """
        INSERT INTO {}
        SELECT *
        FROM {}
        WHERE movie_douban_url='{}';
        """.format(target_table_name, source_table_name, target_url)
        cursor.execute(save_row_query)
        connect_point.commit()
        print('Saved: ', target_url, 'to', target_table_name)


def delete_row_by_url(connect_point=None, table_name='', target_url=''):
    if not connect_point:
        print('Missing connection')
    elif not table_name:
        print('Missing table')
    elif not target_url:
        print('Missing url')
    else:
        cursor = connect_point.cursor(buffered=True)
        delete_row_query = """
        DELETE FROM {}
        WHERE movie_douban_url='{}';
        """.format(table_name, target_url)
        cursor.execute(delete_row_query)
        connect_point.commit()
        print('Deleted: ', target_url)
