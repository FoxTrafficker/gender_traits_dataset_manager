from pymongo import MongoClient
from pathlib import Path
import copy
from tqdm import tqdm
import json
import re
from pymongo import UpdateOne
from pymongo import MongoClient, errors
import gridfs
from pathlib import Path
import copy
from tqdm import tqdm


class MongoDBAPI:
    def __init__(self, db_name: str = 'gender_traits_dataset', collection_name: str = 'fs'):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def backup_collection(self, backup_file):
        with open(backup_file, 'w', encoding='utf-8') as f:
            documents = self.collection.find()

            for doc in documents:
                json_doc = json.dumps(doc, default=str, ensure_ascii=False)
                f.write(json_doc + '\n')
        print(f"集合备份完成，文件保存在: {backup_file}")

    def restore_collection(self, backup_file):
        with open(backup_file, 'r', encoding='utf-8') as f:
            for line in f:
                doc = json.loads(line)
                self.collection.insert_one(doc)
        print(f"集合恢复完成，数据来自: {backup_file}")

    def insert_record(self, record):
        if 'url' in record:
            existing_record = self.collection.find_one({'url': record['url']})
            if existing_record:
                # print(f"url: {record['url']} already exists in {self.db.name} {self.collection.name} ")
                return None

        return self.collection.insert_one(record)


if __name__ == '__main__':
    pass
