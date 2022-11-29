import json
from Levenshtein import distance as levenshtein_distance
import spacy
from processor import FeatureNormalizer

# modify features file if levenstein difference if less than 5 % between the target item and any item in the group?
if __name__ == "__main__":
    output=[]
    data=''
    with open('feature_mapping.json', encoding="utf8") as f:
        data = json.load(f)

    for row in data:
        output.append(FeatureNormalizer(row['FIELD'], row['LOOKUP']))

    # string_to_match = "2 usb & aux-in"
    string_to_match = "multifuktions-lederlenkrad"
    for feature in output:
        print("Checking :" + feature.FIELD)
        l = feature.LOOKUP
        for i in range(0,len(l)):
            if len(l[i]) == 0:
                continue

            relative_ldist = levenshtein_distance(string_to_match, l[i]) / len(l[i]) * 100
            if relative_ldist < 20:
                print("Similarity between '%s' and '%s': %d" % (string_to_match, l[i], relative_ldist))

    pass