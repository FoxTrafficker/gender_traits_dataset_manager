from typing import Dict, Union, List, Optional
from io import BytesIO
import configparser
import yaml
import json

# LABEL_DEFINITION = "config/label_definition.yml"
LABEL_DEFINITION = "../../config/label_definition.yml"


class LabelManager:
    def __init__(self):
        self.label_definitions = self._load_definition()
        self.label = {label: self._process_sub_labels(sub_labels) for label, sub_labels in self.label_definitions.items()}

    @staticmethod
    def _load_definition():
        with open(LABEL_DEFINITION, 'r') as f:
            return yaml.safe_load(f)

    def _process_sub_labels(self, sub_labels):
        if isinstance(sub_labels, dict):
            return {
                "value": "null",
                "sub_labels": {k: self._process_sub_labels(v) for k, v in sub_labels.items()}
            }
        elif isinstance(sub_labels, list):
            return [self._process_sub_labels(item) for item in sub_labels]
        else:
            return {sub_labels: {"value": "null"}}

    def export(self):
        return self.label

    def export_json(self):
        return json.dumps(self.label, indent=4)


class DataObject:
    def __init__(self):
        self.image = None
        self.labels = None
        self.annotation = None
        self.metadata = None

    def set_image(self, image: BytesIO):
        self.image = image

    def set_labels(self, labels: Dict[str, Union[str, Dict]]):
        pass

    def set_annotation(self, annotation: Dict):
        self.annotation = annotation

    def set_metadata(self, metadata: Dict):
        self.metadata = metadata

    def get_labels(self) -> Dict[str, Union[str, Dict]]:
        return self.labels


if __name__ == "__main__":
    label_definition = LabelManager()
    json_result = label_definition.export()
    print(json_result)
