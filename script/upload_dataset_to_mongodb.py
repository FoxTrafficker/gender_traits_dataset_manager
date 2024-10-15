from pymongo import MongoClient, errors
import gridfs
from pathlib import Path
import copy
from tqdm import tqdm


class MongoDBAPI:
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client['gender_traits_dataset']  # Replace with your actual database name
        self.fs = gridfs.GridFS(self.db)
        self.fs_files = self.db['fs.files']  # Collection where file metadata is stored

        # Ensure the unique index on 'metadata.image_name' exists
        self.fs_files.create_index([("metadata.image_name", 1)], unique=True)


if __name__ == '__main__':
    image_folder = Path(r"C:\Users\Hulifanzi\Desktop\datasets")
    mongo_api = MongoDBAPI()
    fs = mongo_api.fs

    LabelDefinition = {'typical_male': None, }

    # Get all files in the folder recursively
    image_paths = list(image_folder.glob('**/*'))

    # Add a progress bar with tqdm
    for image_path in tqdm(image_paths, desc="Processing Images", unit="file"):
        if image_path.is_file() and image_path.suffix in [".jpg", ".png", ".jpeg", ".webp", ".gif", ".bmp"]:
            p = image_path.parts

            true_label = copy.deepcopy(LabelDefinition)
            pseudo_label = copy.deepcopy(LabelDefinition)

            if "Labeled Datasets" in p and "Pseudo-Label Datasets" not in p:
                if "no_male" in p and "with_male" not in p:
                    true_label["typical_male"] = False
                elif "with_male" in p and "no_male" not in p:
                    true_label["typical_male"] = True
                else:
                    raise ValueError(f"Invalid image path: {image_path}")

            elif "Pseudo-Label Datasets" in p and "Labeled Datasets" not in p:
                if "no_male" in p and "with_male" not in p:
                    pseudo_label["typical_male"] = False
                elif "with_male" in p and "no_male" not in p:
                    pseudo_label["typical_male"] = True
                else:
                    raise ValueError(f"Invalid image path: {image_path}")
            else:
                raise ValueError(f"Invalid image path: {image_path}")

            metadata = {
                "image_name": image_path.name,
                "source": None
            }

            if "pixiv" in p:
                metadata["source"] = "pixiv"
            if "gelbooru" in p:
                metadata["source"] = "gelbooru"

            # Try uploading the image; if duplicate image_name exists, MongoDB will raise a DuplicateKeyError
            try:
                with open(image_path, 'rb') as file_data:
                    file_id = fs.put(file_data, true_label=true_label, pseudo_label=pseudo_label, metadata=metadata)
            except gridfs.errors.FileExists:
                print(f"Image {image_path.name} already exists in the database, skipping upload.")
