import configparser
from minio import Minio
from functools import cached_property
from datetime import datetime
from pathlib import Path


# Minio 客户端管理类
class MinioAPI:
    def __init__(self, config_file='config/config.ini'):
        self.config = self._load_config(config_file)
        self.client = Minio(**self.config)

    @staticmethod
    def _load_config(file):
        """加载并返回 MINIO 配置节。"""
        config = configparser.ConfigParser()
        config.read(file)
        return dict(config['MINIO'])

    def ensure_bucket_exists(self, bucket_name):
        """确保 bucket 存在，如果不存在则创建。"""
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")


if __name__ == '__main__':
    client = MinioAPI('../../config/config.ini')
