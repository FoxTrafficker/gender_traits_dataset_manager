from pymongo import MongoClient
from pathlib import Path
import copy
from tqdm import tqdm


class MongoDBAPI:
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client['gender_traits_dataset']  # Replace with your actual database name
        self.fs_files = self.db['fs.files']  # Collection where file metadata is stored


if __name__ == '__main__':
    image_folder = Path(r"C:\Users\Hulifanzi\Desktop\datasets")  # Your dataset path
    mongo_api = MongoDBAPI()
    fs_files = mongo_api.fs_files

    LabelDefinition = {'typical_male': None}

    # Get the total number of files for the progress bar
    total_files = 2393444

    # Iterate over the image paths with a progress bar, without reading all at once
    for image_path in tqdm(image_folder.glob('**/*'), desc="Processing Images", unit="file", total=total_files):
        if image_path.is_file() and image_path.suffix in [".jpg", ".png", ".jpeg", ".webp", ".gif", ".bmp"]:
            p = image_path.parts

            true_label = copy.deepcopy(LabelDefinition)
            pseudo_label = copy.deepcopy(LabelDefinition)

            # Derive correct labels based on the file path
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

            # Search for the document in MongoDB by image_name
            image_name = image_path.name
            document = fs_files.find_one({"metadata.image_name": image_name})

            if document:
                # Check if the labels need updating
                current_true_label = document.get('true_label', {})
                current_pseudo_label = document.get('pseudo_label', {})

                # Update true_label and pseudo_label if they differ from the current ones
                update_fields = {}
                if current_true_label != true_label:
                    update_fields['true_label'] = true_label
                if current_pseudo_label != pseudo_label:
                    update_fields['pseudo_label'] = pseudo_label

                if update_fields:
                    # Perform the update in MongoDB
                    fs_files.update_one(
                        {"_id": document["_id"]},  # Match the document by its unique ID
                        {"$set": update_fields}  # Set the updated fields
                    )
                    # print(f"Updated labels for {image_name}")
                else:
                    pass
                    # print(f"No label changes needed for {image_name}")
            else:
                print(f"Image {image_name} not found in the database.")
