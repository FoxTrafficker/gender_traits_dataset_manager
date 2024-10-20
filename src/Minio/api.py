import configparser
from minio import Minio
from minio.error import S3Error
from io import BytesIO


class MinioAPI:
    def __init__(self, config_path='config/config.ini'):
        self.config = self._load_config(config_path)
        self.client = Minio(**self.config, secure=False)

    @staticmethod
    def _load_config(file):
        """加载并返回 MINIO 配置节。"""
        config = configparser.ConfigParser()
        config.read(file)
        return dict(config['MINIO'])

    def ensure_bucket_exists(self, bucket_name, create_new=True):
        if not self.client.bucket_exists(bucket_name):
            if create_new:
                self.client.make_bucket(bucket_name)
                print(f"Bucket '{bucket_name}' created.")
            else:
                raise Exception(f"Bucket {bucket_name} not exist")
        else:
            pass

    def upload_file(self, bucket_name: str, data: BytesIO, object_name: str, object_id: str | int):
        """
        上传文件到指定的 MinIO bucket 中。

        :param bucket_name: 要上传的目标 bucket 名称。
        :param data: 要上传的字节流（BytesIO 对象）。
        :param object_name: 在 MinIO 中保存的对象名称。
        :param object_id: 从 MongoDB 获得的用于查询的 object_id。
        """
        self.ensure_bucket_exists(bucket_name)

        try:
            self.client.stat_object(bucket_name, object_name)
            # print(f"Object '{object_name}' already exists in bucket '{bucket_name}'.")
            return
        except S3Error as e:
            if e.code != 'NoSuchKey':
                raise Exception(f"Failed to check if object exists. Error: {e}")

        metadata = {'object_id': object_id}

        try:
            self.client.put_object(bucket_name, object_name, data, length=-1, part_size=10 * 1024 * 1024, metadata=metadata)
        except S3Error as e:
            print(f"Failed to upload data to bucket '{bucket_name}'. Error: {e}")

    def fetch_file(self, bucket_name: str, object_name: str):
        response = self.client.get_object(bucket_name, object_name)

        file = BytesIO()
        for data in response.stream(32 * 1024):
            file.write(data)

        file.seek(0)

        response.close()
        response.release_conn()

        return file
