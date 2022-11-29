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

    @staticmethod
    def to_json(feature_normalizer):
        # TODO: make feature_normalizer class field
        out = []
        for i in feature_normalizer:
            out.append( {"FIELD": i.FIELD, "LOOKUP": i.LOOKUP})
        out_str = json.dumps(out, indent=4)
        with open('feature_mapping.json', encoding="utf8", mode="w") as f:
            f.write(out_str)