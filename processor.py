import json
import sqlite3
from sqlite3 import Error
from collections import defaultdict
import re
from datetime import datetime
from os import walk

class FeatureNormalizer:
    def __init__(self, field, lookup):
        self.FIELD = field.lower()
        self.LOOKUP = list(map(str.lower, lookup))

def parseFeaturesFromJsonFile():
    output=[]
    data=''
    with open('feature_mapping.json', encoding="utf8") as f:
        data = json.load(f)

    for row in data:
        output.append(FeatureNormalizer(row['FIELD'], row['LOOKUP']))
        
    return output


FEATURE_NORMALIZERS = parseFeaturesFromJsonFile()
conn = None

def main(debugFlag):
    assert conn != None

    PASS_THROUGH_FIELDS = ['manufacturer', 'modelgroup', 'model', 'cashprice_raw', 'thumbnail',  'vin', 'parking_place', 'location', 'tag', 'id']

    all_files = next(walk("input"), (None, None, []))[2]

    already_scanned = alreadyPresentDays()

    cur = conn.cursor()

    # when creating a parser class we can pass in: debugFlag, already_scanned
    if debugFlag:
        all_files = ['25-06-2022.json']
        already_scanned = []
    
    # should be a method in the parser class
    try:
        for file_name in all_files:
            if file_name == ".DS_Store":
                print("Ignoring file " + file_name)
                continue

            scan_date = parseDateFromJsonFileName(file_name)

            if scan_date in already_scanned:
                # print("Ignoring since it was already scanned: " + file_name)
                continue
           
            # define FEATURE_NORMALIZERS and PASS_THROUGH_FIELDS as dependencies of parser class
            flat = process_date_file(FEATURE_NORMALIZERS, PASS_THROUGH_FIELDS, file_name)

            if flat == None:
                # we have some unmapped values that we don't want to commit partially
                continue

            if debugFlag:
                print(json.dumps(flat,default=str))
            else:
                insert_to_database(cur, flat)

    except Error as e:
        print("Error inserting a new entry")
        print(e)

def insert_to_database(cur, flat):
    for myDict in flat:
        columns = ', '.join(myDict.keys())
        placeholders = ', '.join('?' * len(myDict))
        sql = 'INSERT INTO autos ({}) VALUES ({})'.format(columns, placeholders)
        values = [x for x in myDict.values()]
        cur.execute(sql,values)
    conn.commit()

def process_date_file(FEATURE_NORMALIZERS, PASS_THROUGH_FIELDS, scan_date):
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

        for passThrough in PASS_THROUGH_FIELDS:
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
        print("Aborting inserting for file, since there are some unmapped features. Make sure to map them and extend DB schema.")
        # use set() to deduplicate
        print()
        for ignored_item in set(all_ignored):
            print("'" + ignored_item + "', ")

        print()
        
        return None
    
    flat = flatten_values_array_to_string(processed_cars)
    return flat
    
def open_db_conn():
    conn = None
    try:
        conn = sqlite3.connect("data/all_database.db")
    except Error as e:
        print(e)

    return conn

def flatten_values_array_to_string(processed_cars):
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

### You need to modify the table if introducing additional features
def createSchema():

    create_table_str = """
    CREATE TABLE "autos" ( 
        'index' INTEGER PRIMARY KEY AUTOINCREMENT,
        'model' TEXT,
        'cashprice_raw' INTEGER,
        'mileage_km' INTEGER,
        'licensedate' TIMESTAMP,
        'internal_vehicle_number' INTEGER,
        'abs' TEXT,
        'active_tempomat' TEXT,
        'airbag' TEXT,
        'airbag_deactivate_passenger' TEXT,
        'alloy_wheels' TEXT,
        'automatic' TEXT,
        'bluetooth' TEXT,
        'board_computer' TEXT,
        'central_lock' TEXT,
        'date_order' TIMESTAMP,
        'dead_spot_warn' TEXT,
        'electric_parking_brake' TEXT,
        'electric_windows' TEXT,
        'electronic_immobiliser' TEXT,
        'equipment_level' TEXT,
        'esp' TEXT,
        'fancy_color' TEXT,
        'fancy_driver_seat' TEXT,
        'fancy_sound' TEXT,
        'fancy_steering_wheel' TEXT,
        'fog_lights' TEXT,
        'hand_support_midle' TEXT,
        'hill_assistant' TEXT,
        'id' INTEGER,
        'isofix' TEXT,
        'klima' TEXT,
        'kw' INTEGER,
        'last_maintenance_km' REAL,
        'leather_seats' TEXT,
        'led_lights' TEXT,
        'light_dim_assistant' TEXT,
        'ligths_cleaning' TEXT,
        'location' TEXT,
        'manufacturer' TEXT,
        'modelgroup' TEXT,
        'navigation' TEXT,
        'other' TEXT,
        'panorama_roof' TEXT,
        'parking_assistant' TEXT,
        'parking_place' TEXT,
        'phone_connector' TEXT,
        'radio' TEXT,
        'reverse_camera' TEXT,
        'roof_rails' TEXT,
        'scan_date' TIMESTAMP,
        'seat_heating' TEXT,
        'servo_steering' TEXT,
        'side_mirror_electric' TEXT,
        'sleepiness_sensor' TEXT,
        'split_rear_bank' TEXT,
        'sport_seats' TEXT,
        'start_stop' TEXT,
        'sun_rollo' TEXT,
        'tag' TEXT,
        'tempomat' TEXT,
        'theft_alarm' TEXT,
        'thumbnail' TEXT,
        'tires_fancy' TEXT,
        'traffic_line_assist' TEXT,
        'traffic_sign_recognition' TEXT,
        'trailer_hitch' TEXT,
        'trailer_hitch_preparation' TEXT,
        'trunk_electric' TEXT,
        'usb_aux' TEXT,
        'vin' TEXT,
        'windows_dimmed' TEXT,
        'wireless_charge' TEXT,
        'active_info_display' TEXT,
        'alarm' TEXT,
        'apple_carplay' TEXT,
        'big_gas_tank' TEXT,
        'break_assistant' TEXT,
        'digital_radio' TEXT,
        'drive_assistant' TEXT,
        'driver_profile_assistant' TEXT,
        'four_wheel_drive' TEXT,
        'front_collision_warn' TEXT,
        'front_window_heating' TEXT,
        'hagel_damage' TEXT,
        'region_code' TEXT,
        'smart_key_unlock' TEXT,
        'speed_limit_info' TEXT,
        'storage_organisation' TEXT,
        'surround_view' TEXT,
        'trunk_rollo' TEXT,
        )
    """
    create_index_str = 'CREATE INDEX "ix_autos_index"ON "autos" ("index")'

    conn = None
    try:
        conn = sqlite3.connect("data/all_database.db")
        conn.execute(create_table_str)
        conn.execute(create_index_str)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def addColumnToSchema():
    add_column_str = """
    ALTER TABLE autos ADD COLUMN missed TEXT;
    """
    conn = open_db_conn()
    try:
        conn.execute(add_column_str)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def printAllFilesInFolder():
    all_files = next(walk("input"), (None, None, []))[2]
    print(all_files)

# move to separate file
def alreadyPresentDays():
    assert conn != None

    query = """
    select DISTINCT(scan_date) from autos;
    """
    try:
        cur = conn.cursor()
        cur.execute(query)

        rows = cur.fetchall()
        already_present_dates = [datetime.fromtimestamp(row[0]) for row in rows]
        return already_present_dates
    except Error as e:
        print("Error getting already present days")
        print(e)

def parseDateFromJsonFileName(json_file_name):
    return datetime.strptime(json_file_name.split(".")[0], '%d-%m-%Y')

if __name__ == "__main__":
    conn = open_db_conn()

    try:
        main(False)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

    # dtale.show(pd.DataFrame([1,2,3,4,5]), subprocess=False)
    #main()
    #createSchema()
    #experiment()
    #addColumnToSchema()
    #experiment()

    # Nice to have TODOS
    # - we could map FeatureNormalizers with table creation, since they use the same field names, we would just need to add type info to FNs and maybe name the class better
    pass