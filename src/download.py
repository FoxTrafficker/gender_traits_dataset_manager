from pixiv.api import PixivAPI
from mongodb.api import MongoDBAPI

pixiv = PixivAPI(config_path='../config/config.ini')
mongo = MongoDBAPI()


class PixivDownloader:
    def __init__(self):
        pass


class PixivR18RankingDownloader:
    def __init__(self):
        pass
