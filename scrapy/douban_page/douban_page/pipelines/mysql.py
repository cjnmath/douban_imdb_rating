import dj_database_url
import mysql.connector


class MysqlWriter(object):
    """ test write to MySQL databases
    """
    @classmethod
    def from_crawler(cls, crawler):

        mysql_url = crawler.settings.get('MYSQL_PIPELINE_URL', None)

        return cls(mysql_url=mysql_url)

    def parse_mysql_url(mysql_url):
        """ Parses mysql url and prepares arguments for
            adbapi.ConnectionPool()
        """
        params = dj_database_url.parse(mysql_url)

        connection_kwargs = {}
        connection_kwargs['host'] = params['HOST']
        connection_kwargs['user'] = params['USER']
        connection_kwargs['passwd'] = params['PASSWORD']
        connection_kwargs['database'] = params['NAME']
        # connection_kwargs['port'] = params['PORT']

        connection_kwargs = dict(
                                (k, v)
                                for k, v in connection_kwargs.items()
                                if v)

        return connection_kwargs

    def __init__(self, mysql_url):
        """ Open a MySQL connection pool
        """

        self.mysql_url = mysql_url

        connection_kwargs = MysqlWriter.parse_mysql_url(self.mysql_url)
        self.sql_connection = mysql.connector.connect(**connection_kwargs)
        self.cursor = self.sql_connection.cursor()

    def process_item(self, item, spider):

        sql = """INSERT INTO douban_to_imdb (
        douban_url,
        imdb_url)
        VALUES (%s,%s)
        """
        # movie_cast_cn,
        # movie_director_cn,

        args = (
            ', '.join(item['movie_douban_url']),
            ', '.join(item['movie_imdb_url'])
            # ', '.join(item['movie_region_id']),
            # ', '.join(item['movie_cast_cn']),
            # ', '.join(item['movie_director_cn']),
            # ', '.join(item['movie_douban_score'])
        )
        self.cursor.execute(sql, args)
        self.sql_connection.commit()
        return item
