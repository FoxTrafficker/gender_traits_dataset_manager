from src.pixiv.api import PixivAPI
from src.mongodb.api import MongoDBAPI
from src._minio.api import MinioAPI
from tqdm import tqdm
from urllib.parse import urlparse
from pathlib import Path


class BaseDataManager:
    def __init__(self, config_path):
        self.mongodb = MongoDBAPI()
        self.minio = MinioAPI(config_path=config_path)

    def store_data(self, *args):
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

    def download_artwork(self, artwork_id: str | int):
        """
        artwork_id: the id of an artwork.
        """
        metadata = self.pixiv.get_metadata(artwork_id)

        urls = [page.image_urls['original'] for page in metadata.meta_pages] if metadata.meta_pages else [
            metadata.meta_single_page.get('original_image_url') or metadata.image_urls.get('original')]

        images = {url: self.download(url) for url in urls}

        return metadata, images

    def store_data(self, metadata, images, note: str = None):
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

    def fetch_data(self):
        # todo 首先检查数据库中是否存在
        # todo 如果存在则直接从数据库中提取，如果不存在则从链接中获取。
        pass


if __name__ == '__main__':
    pdm = PixivDataManager('../config/config.ini')
    artwork_ids = pdm.pixiv.get_R18_ranking('2024-10-17')
    for artwork_id in tqdm(artwork_ids):
        pdm.store_data(*pdm.download_artwork(str(artwork_id)))
