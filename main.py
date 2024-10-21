from src.Minio.MinioClientManager import MinioDatasetManager
from src.Pixiv.api import PixivAPI


def test():
    imgs = PixivAPI(config_path='config/config.ini').download_artwork(122210241)
    print(imgs)


if __name__ == '__main__':
    test()
