from src.Pixiv.api import PixivAPI
from src.Mongodb.api import MongoDBAPI
from src.Minio.api import MinioAPI
from tqdm import tqdm
from urllib.parse import urlparse
from pathlib import Path
import time
from datetime import datetime, timedelta


class BaseDataManager:
    def __init__(self, config_path):
        self.mongodb = MongoDBAPI()
        self.minio = MinioAPI(config_path=config_path)

    def store_data(self, *args):
        pass

    def valid_dataset(self, *args):
        pass


class PixivDataManager(BaseDataManager):
    def __init__(self, config_path: str = 'config/config.ini'):
        super().__init__(config_path)
        self.pixiv = PixivAPI(config_path=config_path)
        self.mongodb = MongoDBAPI(db_name='pixiv', collection_name='test')

    def download(self, url: str):
        """
        url: the url of a single image.
        """
        return self.pixiv.download_to_memory(url)

    def download_artwork(self, artwork_id: int):
        """
        artwork_id: the id of an artwork.
        """
        metadata = self.pixiv.get_metadata(artwork_id)

        urls = [page.image_urls['original'] for page in metadata.meta_pages] if metadata.meta_pages else [
            metadata.meta_single_page.get('original_image_url') or metadata.image_urls.get('original')]

        images = {url: self.download(url) for url in urls}

        return metadata, images

    def store_data(self, metadata, images, note=None):
        for url, img in images.items():
            img_name = Path(urlparse(url).path).name

            record = {
                "url": url,
                "metadata": metadata,
                "image_name": img_name,
                "note": note,

                "true_label": {
                    "typical_male": None
                },
                "pseudo_label": {
                    "typical_male": None
                }
            }

            object_id = self.mongodb.insert_record(record)
            self.minio.upload_file(bucket_name="pixiv", data=img, object_name=img_name, object_id=object_id)

    def check_exist(self, artwork_id: int):
        record = self.mongodb.collection.find_one({'metadata.id': artwork_id})
        return bool(record)

    def fetch_data(self, artwork_id: int):
        records = self.mongodb.collection.find({'metadata.id': artwork_id}).to_list()
        if len(records) > 0:
            metadata = records[0]['metadata']
            images = {r['url']: self.minio.fetch_file(bucket_name="pixiv", object_name=r['image_name']) for r in records}
            source = 'database'
        else:
            metadata, images = self.download_artwork(artwork_id)
            source = 'pixiv'

        return metadata, images, source

    def valid_dataset(self):
        # todo 验证单一性
        # todo mongodb中url不应该重复，artwork_id+page_count不应该重复。
        # todo minio中相图像名不应该重复，object_id不应该重复。

        # todo 验证一致性
        # todo mongodb中出现的元数据的图像应该全部在minio中可以找到，且object_id应该一致。
        # todo minio中出现的图像应该可以在minio中找到元数据和标签。

        # todo 验证完整性
        # todo mongodb中字段应该完整。同一个artwork的page应该完整。
        pass


def main():
    pdm = PixivDataManager('../config/config.ini')
    start_date = datetime.strptime("2021-02-21", "%Y-%m-%d").date()
    end_date = datetime.today().date()
    current_date = start_date

    while start_date < end_date:
        print(datetime.strftime(current_date, "%Y-%m-%d"))

        artwork_ids = pdm.pixiv.get_R18_ranking(datetime.strftime(current_date, "%Y-%m-%d"))

        for artwork_id in tqdm(artwork_ids):
            try:
                metadata, images, source = pdm.fetch_data(artwork_id=artwork_id)
            except Exception as e:
                print(e, 'try again')
                metadata, images, source = pdm.fetch_data(artwork_id=artwork_id)

            if source == 'pixiv':
                pdm.store_data(metadata, images, note={"mode": "daily_r18", "content": "illust", "date": datetime.strftime(current_date, "%Y%m%d")})
                time.sleep(5)
        time.sleep(300)

        current_date += timedelta(days=1)


if __name__ == '__main__':
    main()
