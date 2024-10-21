import configparser
from minio import Minio
from functools import cached_property
from datetime import datetime
from pathlib import Path
from .data_object import DataObject


# Minio 客户端管理类
class MinioClient:
    def __init__(self, config_file="config.ini"):
        self.config = self._load_config(config_file)

    @staticmethod
    def _load_config(file):
        """加载并返回 MINIO 配置节。"""
        config = configparser.ConfigParser()
        config.read(file)
        return config['MINIO']

    @cached_property
    def client(self):
        """懒加载 Minio 客户端实例，并缓存。"""
        return Minio(
            self.config['endpoint'],
            access_key=self.config['access_key'],
            secret_key=self.config['secret_key'],
            secure=self.config.getboolean('secure')
        )

    def ensure_bucket_exists(self, bucket_name):
        """确保 bucket 存在，如果不存在则创建。"""
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")


# Minio 数据集管理类
class MinioDatasetManager:
    def __init__(self, client: MinioClient):
        self.MinioClient = client

    @staticmethod
    def _generate_path(filetype: str, date: datetime):
        """根据文件类型和日期生成对象路径。"""
        date_path = filetype + date.strftime("/%Y/%m/%d")
        return date_path

    def _upload(self, object_name: str, file_path: str, bucket_name: str):
        self.MinioClient.ensure_bucket_exists(bucket_name)
        self.MinioClient.client.fput_object(bucket_name, object_name, file_path)
        print(f"Uploaded {file_path} to {object_name} in bucket {bucket_name}")


# 使用示例
if __name__ == '__main__':
    # 创建 MinioClient 实例
    client_manager = MinioClient()

    # 创建 MinioDatasetManager 实例，传入 client_manager
    dataset_manager = MinioDatasetManager(client_manager)

    # 创建 DataObject 对象并设置数据
    example_data_object = DataObject()
    example_data_object.set_image("path/to/image1.jpg")
    example_data_object.set_label(["label1", "label2"])
    example_data_object.set_annotation({"key": "value"})
    example_data_object.set_metadata({"metadata_key": "metadata_value"})

    # 上传 DataObject 对象
    dataset_manager.upload_data_object(example_data_object, filetype="images", date=datetime(2023, 10, 3))
