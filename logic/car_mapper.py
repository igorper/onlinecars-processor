
from datetime import datetime
import json
from collections import defaultdict
import re


class CarMapper:
    PASS_THROUGH_FIELDS = ['manufacturer', 'modelgroup', 'model', 'cashprice_raw', 'thumbnail',  'vin', 'parking_place', 'location', 'tag', 'id']

    def __init__(self) -> None:
        pass

    def process_date_file(self, FEATURE_NORMALIZERS, scan_date):
        print(">> Processing file " + scan_date)
        # a different implementation can just pass out some dummy data
        # we can also have this unit testable by providing a single file (or in mem representation) and checking in the end that expected values are correct
        with open('input/' + scan_date, encoding="utf8") as f:
            data = json.load(f)

        all_ignored = []
        processed_cars = []
        for car in data['data']['cars']:
            # skip nodes without cars (advertising)
            if 'cashprice_raw' not in car:
                print('Skipping node with name: ' + car['name'])
                continue
        
            features=car['features'] + ',' + car['top_features']
            features_arr = features.split(",")
            processed_car = defaultdict(list)

            for passThrough in self.PASS_THROUGH_FIELDS:
                processed_car[passThrough] = car[passThrough]

            # parse numbers for some feature
            processed_car['kw'] = int(car['kw'].split(" ")[0])
            processed_car['date_order'] = datetime.strptime(car['date_order'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp() if car['date_order'] is not None else None
            processed_car['licensedate'] = datetime.strptime(car['licensedate'], '%Y-%m-%d').timestamp()
            processed_car['mileage_km'] = int(car['mileage'].replace(".", "").split(" ")[0])
            processed_car['scan_date'] = datetime.strptime(scan_date.split(".")[0], '%d-%m-%Y').timestamp()
            processed_car['internal_vehicle_number'] = car['internal-vehicle-number']

            if car['manufacturer'] == 'VW':
                if 'HL' in car['model']:
                    processed_car['equipment_level'] = "HighLine"
                elif 'CL' in car['model'] or 'Comfortline' in car['model'] or 'Comfortl.' in car['model']:
                    processed_car['equipment_level'] = "ComfortLine"

            for feature in features_arr:
                feature = feature.strip().lower()

                detected = False
                for FEATURE_NORMALIZER in FEATURE_NORMALIZERS:
                    # deduplicate
                    if feature in FEATURE_NORMALIZER.LOOKUP:
                        if feature not in processed_car[FEATURE_NORMALIZER.FIELD]:
                            processed_car[FEATURE_NORMALIZER.FIELD].append(feature)
                        detected = True

                if detected is False:
                    if "letzte wartung" in feature or "letzt wartung" in feature:
                        remove_dot_string = feature.replace(".", "")
                        # creative regex string, since there are sometimes typos on the web page in the description
                        last_maintenance_km = re.search(r"letzte? wartung bv?i?e?i? (\d*) km", remove_dot_string).group(1)
                        processed_car['last_maintenance_km'] = int(last_maintenance_km)
                    else:
                        # undetected features go into a missed list for monitoring
                        processed_car['missed'].append(feature)
                        all_ignored.append(feature)
            
            processed_cars.append(processed_car)

        if len(all_ignored) > 0:
            # TODO: use lev distance to map unrecognized features with closest matches (if the difference is less than 20 %), modify feature json and close
            # First step, just rerun, in later versions we can reload the features map and retry

            print("Aborting inserting for file, since there are some unmapped features. Make sure to map them and extend DB schema.")
            # use set() to deduplicate
            print()
            for ignored_item in set(all_ignored):
                print("\"" + ignored_item + "\", ")

            print()
            
            return None
        
        flat = self.__flatten_values_array_to_string(processed_cars)
        return flat

    def __flatten_values_array_to_string(self, processed_cars):
        flat = []
        for pc in processed_cars:
            pc_flat = {}
            for k in pc:
                #pc_flat[k] = ','.join(pc[k])
                if isinstance(pc[k], list):
                    pc_flat[k] = ";".join(pc[k])
                else:
                    pc_flat[k] = pc[k]
            flat.append(pc_flat)
        return flat
