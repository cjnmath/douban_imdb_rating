import pymongo


class MongoPipeline(object):

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.collection_name = 'all_data'

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'test')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.db[self.collection_name].update_one(
                    {'movie_imdb_url': item['url'][0]},
                    {'$set': {
                            'director': item.get('director'),
                            'writer': item.get('writer'),
                            'starts': item.get('starts'),
                            'cast': item.get('cast'),
                            'imdb_score': item.get('imdb_score'),
                            'description': item.get('description'),
                            'keyword': item.get('keyword'),
                            'genre': item.get('genre'),
                            'country': item.get('country'),
                            'language': item.get('language'),
                            'length': item.get('length'),
                            'budget': item.get('budget'),
                            'Worldwide_Gross': item.get('Worldwide_Gross')}})
        return item
