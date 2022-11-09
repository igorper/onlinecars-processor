import json

class FeatureNormalizer:
    def __init__(self, field, lookup):
        self.FIELD = field.lower()
        self.LOOKUP = list(map(str.lower, lookup))

    @staticmethod
    def parse_from_json():
        output=[]
        data=''
        with open('feature_mapping.json', encoding="utf8") as f:
            data = json.load(f)

        for row in data:
            output.append(FeatureNormalizer(row['FIELD'], row['LOOKUP']))

        return output