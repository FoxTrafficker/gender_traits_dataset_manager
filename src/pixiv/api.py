import io
import json
import shutil
import configparser
import os

from pixivpy3 import *
from pathlib import Path
from PIL import Image
from typing import List, Dict


class PixivAPI(AppPixivAPI):
    def __init__(self, config_path: str = 'config/config.ini'):
        super().__init__()
        self.config_path = config_path
        self._login()

    def _login(self):
        config = configparser.ConfigParser()
        config.read(self.config_path)
        self.auth(refresh_token=config['PIXIV']['refresh_token'])

    def download_to_memory(self, url):
        file = io.BytesIO()
        self.download(url, fname=file)
        file.seek(0)
        return file

    def get_metadata(self, artwork_id: str | int) -> AppPixivAPI.illust_detail:
        return self.illust_detail(artwork_id).illust

    def get_R18_ranking(self, date: str = '2024-10-13') -> set[int]:
        return set(artwork.id for offset in range(0, 100, 20) for artwork in self.illust_ranking('day_r18', offset=offset, date=date).illusts)


if __name__ == '__main__':
    pixiv = PixivAPI(config_path='../../config/config.ini')
    artwork_ids = pixiv.get_R18_ranking()
    metadata = pixiv.get_metadata(123265856)
    print(metadata)
