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

FEATURE_NORMALIZERS = [
        FeatureNormalizer('usb_aux', ['usb und aux anschluss', 'usb-schnittstelle & aux-ix', '2 usb schnittstellen & aux in', 'usb schnittstelle', 'usb-schnittstelle & aux-in', 'usb schnittstelle & aux in', 'usb-schnittstellen inkl aux-in', 'usb & aux-in', 'usb-schnittstellen', 'aux-in und usb' ,'2 usb-c-schnittstellen', '2 usb-schnittstellen', '2 usb-schnittstellen & aux-in', '2 usb-schnittstellen aux-in', 'aux-in', 'usb-schnittstelle', 'usb-schnittstelle inkl. aux-in', 'usb-schnittstellen aux-in', 'usb-schnittstelle aux-in', 'usb-schnittstelle auch f\u00fcr ipod/iphone']),
        FeatureNormalizer('tires_fancy', ['amg-räder 18 zoll 225 40 18', '17 zoll räder 225 45 17', '18 zoll r\u00e4der 235 45 18', '18 zoll amg r\u00e4der', '19 zoll r\u00e4der 235 40 19']),
        FeatureNormalizer('klima', ['3 zonen-klimaautomatik', 'climatronic', '3-zonen klimaautomtik', 'kimaautomatik','3 zonen klimaanlage', '3 zonen klima', '3-zonen klimaautomatik', 'klimaanlage', 'klimaautomatik', '3-zonne klimaautomatik', 'standheizung und -l\u00fcftung']),
        FeatureNormalizer('abs', ['abs']),
        FeatureNormalizer('active_tempomat', ['autoamtische-distanzregelung', 'abstandsradar', 'automatische distanzregelung']),
        FeatureNormalizer('airbag', ['seitenairbags', 'kopfairbagsystem für front- und fondpassagiere inkl. seitenairbags vorn', 'kopfairbagsystem inkl. seitenairbags vorn', 'airbag', 'beifahrer airbag']), # join both airbag
        FeatureNormalizer('airbag_deactivate_passenger', ['beifahrerairbag-deaktivierbar', 'deaktivierungsschalter f\u00fcr beifahrerairbag' ,'airbag deaktivierung beifahrer', 'beifahrerairbag deaktivierbar', 'beifahrerairbag-deaktivierung', 'beifahrerairbag deaktiviebrar']),
        FeatureNormalizer('alarm', ['alarmanlage']),
        FeatureNormalizer('four_wheel_drive', ['allrad', 'allradantrieb']),
        FeatureNormalizer('trailer_hitch', ['anhängekupplung', 'anh\u00e4ngerkupplung', 'anh\u00e4ngevorrichtung anklappbar', 'anhaengerkupplung']),
        FeatureNormalizer('trailer_hitch_preparation', ['anhaengevorrichtung', 'anh\u00e4ngevorrichtung']),
        FeatureNormalizer('apple_carplay', ['carplay', 'apple carplay']),
        FeatureNormalizer('side_mirror_electric', ['außenspiegel beheizbar', 'außenspiegel elektr. anklappbar', 'außenspiegel elektr. einstell- anklapp- beheizbar', 'außenspiegel heizbar und elektrisch verstellbar','innen- und aussenspiegel automat. abblendbar', 'außenspiegel elektrisch einstell-/anklapp-/beheizbar', 'außenspiegel elektr. einstell- und beheizbar', 'außenspiegel elektrisch einstell- anklapp- und beheizbar', 'außenspiegel elektr. einstell- beheizbar mit memoryfunktion', 'außenspiegel elektrisch einstell- und beheizbar auf fahrerseite automatisch abblendend', 'außenspiegel elektrisch einstell- anklappbar', 'aussenspiegel li. u. re. abklappbar', 'au\u00dfenspiegel elektr. einstell- beheiz- und anklappbar', 'au\u00dfenspiegel auf fahrerseite automatisch abblendend', 'au\u00dfenspiegel anklapp- und beheizbar', 'au\u00dfenspiegel anklapp und beheizbar' ,'au\u00dfenspiegel anklapp- und beheizbar auf fahrerseite abblendend', 'au\u00dfenspiegel elektr. anklapp- und beheizbar', 'au\u00dfenspiegel elektrisch einstell- und beheizbar', 'aussenspiegel elektr. beheizbar', 'aussenspiegel elektrisch', 'au\u00dfenspiegel elektrisch anklappbar', 'au\u00dfenspiegel elektrisch einstell- anklapp- beheizbar']),
        FeatureNormalizer('parking_assistant', ['aussparkassistent', 'einparkhilfe vorne vorne & hinten', 'parklenkassistent und einparkhilfe', 'einparkhilfe vorne & hitnen', 'einpakrhilfe vorne & hinten', 'auspark- und parklenkassistent', 'einparkhilfe vorn & hinten', 'einparkhilfe vorne  & hinten', 'einaparkhilfe vorne & hinten', 'einparkhilfe hi. + vo.', 'parkassist', 'parklenkassistent', 'parklenkassistent inkl. einparkhilfe','einparkhilfe vorne und hinten', 'ausparkassistent', 'einparkhilfe', 'einparkhilfe  vorne & hinten', 'einparkhilfe vorn und hinten', 'einparkhilfe vorne & hinten', 'aktiver parkassistent', 'einparkhilfe hinten', 'parkassistent']),
        FeatureNormalizer('other', ['r-line', 'federung hinten mit vollautomatischem niveauregulierungssystem', 'real time traffic information', 'reifenreparatur-set', 'fahrerlebnisschalter inkl. eco pro', 'standheizung', 'unfall fahrzeug', 'standheizung/-lüftung', 'verkauf nur an gewerbe', '===', 'variable sportlenkung', 'r-line interieur', 'sportfahwerk', 'r-line exterieur', 'schiebetür links & rechts', 'schiebetür links', 'amg sportpaket', 'dc-schnellladen', 'waermepumpe', 'ac-laden professional', 'schiebetür rechts', 'wärmepumpe', 'm spotpaket', 'aus damenhand', 'm sportpaket', 'luxury-line', 'sportpaket amg', '7-sitzer', 'luxury line', 'm-sportpaket', 'm ärodynamikpaket', 'm sportfahrwerk', 'durchladesystem', 'klapptische an den rückseiten der vordersitze', 'nightpaket', 'sportpaket', 'avantgarde-paket exterieur', 'eu spezifische zusatzumfaenge', 'nigthpaket', 'amg-sportpaket', 'luxury-paket interieur', 'anhängerrangierassistent', 'schlechtwegefahrwerk', 'durchlademoeglichkeit', 'sportfahrwerk', 'basis', 'crimson red metallic', 'sport line', 'sportline']),
        FeatureNormalizer('hill_assistant', ['berganfahrassistent']),
        FeatureNormalizer('bluetooth', ['bluetooth', 'bluetoth']),
        FeatureNormalizer('board_computer', ['bordcomputer']),
        FeatureNormalizer('break_assistant', ['bremsassistent']),
        FeatureNormalizer('roof_rails', ['dachreling', 'dachreling schwarz', 'dachreling silber eloxiert']),
        FeatureNormalizer('theft_alarm', ['diebstahlwarnanlage']),
        FeatureNormalizer('digital_radio', ['digitaler radioempfang']),
        FeatureNormalizer('drive_assistant', ['active guard', 'kollisionswarner m. aktivem bremseingriff', 'anfahrhilfe-/ abfahrhilfe', 'driving assistant']),
        FeatureNormalizer('electric_windows', ['elektr. fensterheber']),
        FeatureNormalizer('electric_parking_brake', ['auto-hold-funktion', 'parkbremse elektrisch inkl. auto-hold-funktion', 'parkbremse elektr.', 'elektrische parkbremse', 'elektronische parkbremse inkl. auto-hold-funktion', 'parkbremse elektr. inkl. auto-hold-funktion', 'parkbremse elektrisch']),
        FeatureNormalizer('esp', ['elektronisches stabilit\u00e4tsprogramm']),
        FeatureNormalizer('fancy_driver_seat', ['fahrerlehne elektr. mit massagefunktion', 'lehneneinstellung elektrisch fahrersitz', 'fahrerlehne mit massagefunktion', 'fahrersitzlehne elektr. verstellbar', 'lendenwirbelstützen vorn auf fahrerseite elektrisch einstellbar', 'fahreristzlehne mit massage', 'lendenwirbelstütze elektr.', 'fahrersitz mit massagefunktion', 'sitzeinstellung elektr.', 'linke vordersitzlehne mit massagefunktion', 'lordosenstützen vorne', 'sportmultifunktions-lederlenkrad', 'massagefunktion', 'lordosenverstellung', 'massage- und memory fahrerseite',  'massage- und memoryfunktion fahrerseite', 'massage- und memoryfunktion auf fahrerseite', 'fahrersitzlehne elektr. mit massagefunkton', 'vordersitze elektrisch einstellbar', 'sporsitze', 'fahrersitzlehne elekt. mit massagefunktion', 'sitzeinstellung elektr. mit memory', 'massagefunktion auf fahrerseite','fahrersitzlehen elektr. einstellbar','fahrprogrammwahlschalter','fahrersitzlehne elektr. einstellbar mit massage','fahrersitz elektr. mit memoryfunktion','fahrersitz mit massage und memory', 'fahrersitz mit memory und massage', 'fahrersitzlehne elektr.', 'fahrersitzlehne elektr. einstellbar', 'fahrersitzlehne elektr. einstellbar mit massagefunktion', 'fahrersitzlehne mit massage', 'lendenwirbelst\u00fctzen auf fahrerseite elektrisch einstellbar mit massagefunktion', 'linke vordersitzlehne mit massage', 'lordosenst\u00dctze fahrer\/beifahrer', 'lordosenstuetze vorne', 'vordersitze elektr. einstellbar', 'fahrersitzlehne elektr. mit massagefunktion', 'fahrersitzlehne mit massagefunktion', 'lendenwirbelst\u00fctzen vorn', 'lendenwirbelst\u00fctzen vorne', 'sitzverstellung elektr. mit memory', 'sitzverstellung elektr.mit memory', 'lordosenst\u00fctze', 'lordosenst\u00fctze fahrer/beifahrer', 'lordosenstuetzen vorne']),
        FeatureNormalizer('driver_profile_assistant', ['Fahrprofilauswahl']),
        FeatureNormalizer('light_dim_assistant', ['innenspiegel autom abblendend', 'innenspiegel autom abblendend','außenspiegel autom abblendend', 'dynamische fernlichtregulierung', 'Fernlichtassistent', 'Innenspiegel automatisch abblendend']),
        FeatureNormalizer('front_collision_warn', ['kollisionswarner mit aktivem bremseingriff', 'Frontkollisionswarner', 'kollisionswarner']),
        FeatureNormalizer('trunk_electric', ['automatische heckklappenbetaetigung', 'heckklappe elektrisch', 'gepäckraumklappe elektrisch', 'Gep\u00e4ckraumklappe elektr.', 'Gep\u00e4cksraumklappe elektr.','']),
        FeatureNormalizer('isofix', ['isfoix', 'Isofix']),
        FeatureNormalizer('trunk_rollo', ['Laderaumrollo']),
        FeatureNormalizer('led_lights', ['scheinwerfer', 'led nebelscheinwerfer', 'xenon scheinwerfer', 'adaptive led-scheinwerfer', 'xenon-scheinwerfer', 'LED-Scheinwerfer', 'adaptiver led-scheinwerfer', 'led scheinwerfer']),
        FeatureNormalizer('leather_seats', ['teilleder sporsitze beheizbar', 'leder-/alcantara sportsitze beheizbar', 'leder/alcantara', 'leder sporsitze beheizbar', 'teilledersitze beheizbar', 'leder-alcantara sportsitze beheizbar', 'leder', 'teilleder sportsitze', 'teilleder', 'teillederausstattung', 'teilleder sportsitze beheizbar', 'leder/-alcantara sportsitze', 'ledersportsitze beheizbar', 'teilleder-ausstattung', 'Leder Sportsitze beheizbar', 'Leder-\/Alcantara', 'Leder-\/Alcantara Sportsitze beheizbar', 'Leder-Alcantara', 'Lederausstattung', 'Lederaustattung', 'leder/-alcantara', 'leder/-alcantara sportsitze beheizbar']),
        FeatureNormalizer('fancy_steering_wheel', ['r-line multifunktions-sportlederlenkrad mit schaltwippen', 'multifunktions-sportlederlenkrad', 'multifunktions-lederlenkrad beheizbar', 'multifunktions-lederlenrkad', 'multifunktionslenkrad in leder beheizbar', 'r-line multifunktions-sportlederlenkrad', 'multifunktion fuer lenkrad', 'm m multifunktions-lederlenkrad', 'mutlifunktions-lederlenkrad', 'm multifunktions-lederlenkrad', 'sport-lederlenkrad', 'm lederlenkrad', 'multifunktionslederlenkrad', 'multifunktionslenkrad in leder mit schaltwippen','Lederlenkrad', 'Lenkradheizung', 'Multifunktions-Lederlenkrad', 'Multifunktions-Lederlenkrad mit Schaltwippen', 'Multifunktionslenkrad', 'Multifunktionslenkrad in Leder', 'Schaltwippen am Lenkrad', 'SPORT-MULTIFUNKTIONSLEDERLENKRAD']),
        FeatureNormalizer('alloy_wheels', ['amg-leichtmetallräder', 'alur\u00e4der', 'Leichtmetallfelgen', 'LEICHTMETALLR\u00c4DER', 'Leichtmetallr\u00e4der', 'r-line alur\u00e4der']),
        FeatureNormalizer('sleepiness_sensor', ['müdigkeisterkennung', 'attention assist', 'M\u00fcdigkeitserkennung']),
        FeatureNormalizer('hand_support_midle', ['fahrerlehne elektrisch einstellbar', 'fahrerlehne elektr. einstellbar', 'mittelarmlehne vorn e', 'mittelarmlehne vorne & hinten', 'Mittelarmlehne vorn', 'Mittelarmlehne vorne', 'armauflage vorne verschiebbar', 'beifahrersitzlehne komplett umklappbar', 'mittelarmlhene vorn']),
        FeatureNormalizer('navigation', ['navigaiton', 'naviagtion', 'navigationssystem', 'Navigation']),
        FeatureNormalizer('fog_lights', ['Nebelscheinwerfer', 'Nebelscheinwerfer und Abbiegelicht', 'LED-Nebelscheinwerfer', 'LED-Nebelscheinwefer']),
        FeatureNormalizer('reverse_camera', ['R\u00dcCKFAHRKAMERA', 'R\u00fcckfahrkamera']),
        FeatureNormalizer('split_rear_bank', ['rücksitzbank asymmetrisch umklappbar', 'rücksitzbank asymmetrisch teilbar längs verschieb- und klappbar', 'rücksitzbank geteilt umklappbar', 'R\u00fccksitzbank umklappbar', 'R\u00fccksitzbank ungeteilt Lehne asymmetrisch geteilt umklappbar', 'R\u00fccksitzbank ungeteilt Lehne asymmetrisch umklappbar', 'R\u00fccksitzbanklehne asymmetrisch geteilt umklappbar', 'R\u00fccksitzlehne umklappbar', 'durchladem\u00f6glichkeit', 'r\u00fccksitzlehne asymmetrisch geteilt umklappbar', 'r\u00fccksitzlehne geteilt umklappbar']),
        FeatureNormalizer('radio', ['radio standard', 'Radio']),
        FeatureNormalizer('region_code', ['Regionscode']),
        FeatureNormalizer('windows_dimmed', ['sonnenschutzverglasung', 'Scheiben abgedunkelt']),
        FeatureNormalizer('ligths_cleaning', ['scheinwerferreinigungsanlage', 'scheinwerfer - reinigungsanlage', 'Scheinwerfer-Reinigungsanlage', 'SCHEINWERFER-WASCHANLAGE']),
        FeatureNormalizer('servo_steering', ['progressivlenkung', 'Servolenkung']),
        FeatureNormalizer('seat_heating', ['vordersitze beheizbar', 'sporsitze beheizbar', 'leder-sportsitze beheizbar', 'teilleder-sportsitze beheizbar', 'sitzheizung vorne & hinten', 'Sitzheizung', 'Sitzheizunug', 'Leder Sportsitze beheizbar', 'Leder-\/Alcantara Sportsitze beheizbar', 'Sportsitze beheizbar', 'leder/-alcantara sportsitze beheizbar']),
        FeatureNormalizer('sun_rollo', ['sonnenschutzrollo an den türscheiben hinten', 'Sonnenschutzrollo']),
        FeatureNormalizer('sport_seats', ['sportsitze vorn', 'Sportsitze', 'Sportsitze beheizbar']),
        FeatureNormalizer('traffic_line_assist', ['spurhalte- stau- ausparkassistent', 'spurhalte-assistent', 'spurhaltewarnung', 'spurhalte und spurwechselwarnung', 'spurhalte- spurwechsel- auspark- assistent', 'spurhalte- und spurwechselassistent', 'stauassistent','Spurhalteassistent', 'SPURVERLASSENSWARNER', 'Spurwechselassistent', 'SPURWECHSELWARNER', 'Spurwechselwarnung']),
        FeatureNormalizer('start_stop', ['start/stop system', 'eco start-stop funktion', 'Start-\/Stopp-Technologie', 'Start-Stopp-System', 'Start-Stopp-System mit Bremsenergie-R\u00fcckgewinnung', 'Start\/Stop System', 'eco start-stop-funktion']),
        FeatureNormalizer('phone_connector', ['telefonschittstelle', 'telefonscnittstelle', 'Telefonschnittstelle', 'audi phone box']),
        FeatureNormalizer('tempomat', ['Tempomat']),
        FeatureNormalizer('dead_spot_warn', ['tot-winkel warner', 'totwinkel-assistent', 'totwinkelassistent', 'Toter Winkel-Warner', 'Totwinkel-Warner','totwinkelerkennung']),
        FeatureNormalizer('traffic_sign_recognition', ['Verkehrszeichenerkennung']),
        FeatureNormalizer('electronic_immobiliser', ['wegfahrsperre elektr.', 'Wegfahrsperre elektr', 'Wegfahrsperre elektronisch']),
        FeatureNormalizer('wireless_charge', ['wirless charging', 'wireless charigng', 'wirelss charging', 'Wireless charging', 'Wireless-Charging']),
        FeatureNormalizer('central_lock', ['Zentralverriegelung']),
        FeatureNormalizer('storage_organisation', ['ablagenpaket']),
        FeatureNormalizer('active_info_display', ['digital cockpit pro', 'digital-cockpit pro', 'multifunktionales instrumentendisplay', 'digital-tacho', 'active info display']),
        FeatureNormalizer('automatic', ['auotmatik', '7-gang-automatikgetriebe', 'automatik', 'automatikgetriebe']),
        FeatureNormalizer('fancy_color', ['mangangrau metallic', 'indiumgrau metallic', 'atlantic blue metallic', 'superfarbe black rubin', 'pure white', 'white silver metallic', 'uranograu', 'pyramid gold metallic', 'melbournerot metallic', 'mineralweiss metallic', 'sparkling brown metallic', 'cavansitblau - metalliclack', 'superfarbe - pyramid gold metallic', 'megafarbe - blue dusk metallic', 'm hochglanz shadow line', 'orientbraun - metalliclack', 'platinsilber metallic', 'black rubin', 'atlantik blue metallic', 'black sapphire metallic', 'deep black perleffekt', 'tornadorot', 'reflexsilber metallic', 'mineralgrau metallic']),
        FeatureNormalizer('hagel_damage', ['hagelschaden']),
        FeatureNormalizer('head_up_display', ['head-up-display', 'head-up display']),
        FeatureNormalizer('fancy_sound', ['dynaudio soundsystem', 'harman/kardon', 'soundsystem harman/kardon', 'harman kardon soundsystem', 'harman/kardon soundsystem', 'harman/kardon surround sound system', 'hifi lautsprechersystem']),
        FeatureNormalizer('front_window_heating', ['frontscheibe drahtlos beheizbar']),
        FeatureNormalizer('smart_key_unlock', ['komortzugang', 'direktstart', 'keyless-start', 'komfortzugang']),
        FeatureNormalizer('big_gas_tank', ['grösserer kraftstofftank', 'groesserer kraftstofftank', 'kraftstoffbehaelter mit grossem inhalt', 'grosser scr-tankbehaelter', 'kraftstoffbeh\u00e4lter mit grossem inhalt']),
        FeatureNormalizer('panorama_roof', ['panorama schiebedach', 'panorama-ausstell-/schiebedach', 'panorama/-schiebedach', 'panorama glasdach', 'panorama-/schiebedach', 'panorama-glasdach', 'panorama-schiebedach', 'schiebedach']),
        FeatureNormalizer('surround_view', ['360 grad kamera', 'surround view', '360-grad kamera']),
        FeatureNormalizer('speed_limit_info', ['speed limit info']),
    ]

conn = None

def main(debugFlag):
    assert conn != None

    PASS_THROUGH_FIELDS = ['manufacturer', 'modelgroup', 'model', 'cashprice_raw', 'thumbnail',  'vin', 'parking_place', 'location', 'tag', 'id']

    all_files = next(walk("input"), (None, None, []))[2]

    already_scanned = alreadyPresentDays()

    cur = conn.cursor()

    if debugFlag:
        all_files = ['25-06-2022.json']
        already_scanned = []
    try:
        for file_name in all_files:
            if file_name == ".DS_Store":
                print("Ignoring file " + file_name)
                continue

            scan_date = parseDateFromJsonFileName(file_name)

            if scan_date in already_scanned:
                # print("Ignoring since it was already scanned: " + file_name)
                continue
           
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