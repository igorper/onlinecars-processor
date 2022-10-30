import json
from sqlite3 import Error
from os import walk
from venv import create
from logic.car_mapper import CarMapper
from outbound.sqlite_layer import DataRepository, LoggingRepository, SqlLiteBackend
from datetime import datetime

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

FEATURE_NORMALIZERS = FeatureNormalizer.parseFeaturesFromJsonFile()

# TODO: move to separate file/package
class InputRepository:
    def items_to_process(self, already_scanned):
        pass

class DebugInputRepository(InputRepository):

    def items_to_process(self, already_scanned):
        return ['25-06-2022.json']

class DiskInputRepository(InputRepository):
    def __init__(self):
        pass

    def items_to_process(self, already_scanned):
        all_file_names = next(walk("input"), (None, None, []))[2]

        all_files_without_scanned = self.__remove_allready_scanned_days(all_file_names, already_scanned)
        
        return all_files_without_scanned


    # can be unit tested
    # think if it makes sense to make the input parameters of the same type
    def __remove_allready_scanned_days(self, all_files: list[str], already_scanned: list[datetime]):
        assert all_files != None
        assert already_scanned != None

        all_files_without_scanned = all_files
        all_files_without_scanned.remove(".DS_Store")

        print("All files available for scanning (count: " + str(len(all_files_without_scanned)) + ")")
        # print(all_files_without_scanned)

        to_remove=[]
        for file_name in all_files_without_scanned:
            file_date = self.__filenameToDate(file_name)
            if file_date in already_scanned:
                to_remove.append(file_name)

        print("Ignoring already scanned files (count: " + str(len(to_remove)) + ")")
        # print(to_remove)

        for rm in to_remove:
            all_files_without_scanned.remove(rm)

        return all_files_without_scanned

    def __filenameToDate(self, json_file_name):
        return datetime.strptime(json_file_name.split(".")[0], '%d-%m-%Y')

def main(input: InputRepository, db: DataRepository):
    already_scanned = db.alreadyPresentDays()

    all_files_without_scanned = input.items_to_process(already_scanned)

    car_mapper = CarMapper()

    print("Files left for scanning (count: " + str(len(all_files_without_scanned)) + ")")
    # print(all_files_without_scanned)

    # should be a method in the parser class
    all_to_insert = []
    for file_name in all_files_without_scanned:
        # define FEATURE_NORMALIZERS and PASS_THROUGH_FIELDS as dependencies of parser class
        mapped_auto = car_mapper.process_date_file(FEATURE_NORMALIZERS, file_name)

        if mapped_auto != None:
            all_to_insert.append(mapped_auto)    

    for item in all_to_insert:
        try:
            db.insert_to_database(item)
        except Error as e:
            print("Error inserting a new entry")
            print(e)

def printAllFilesInFolder():
    all_files = next(walk("input"), (None, None, []))[2]
    print(all_files)

if __name__ == "__main__":
    debug_flag = True

    #db: DataRepository = LoggingRepository()
    #input: InputRepository = DebugInputRepository()

    db: DataRepository = SqlLiteBackend()
    input: InputRepository = DiskInputRepository()

    db.open_db_conn()

    try:
        db_exists = db.db_exists()
        if db_exists == False:
            print("DB doesn't exist yet, intializing it first")
            print("Creating table ...")
            db.createSchema(FEATURE_NORMALIZERS)
        
        main(input, db)
    except Error as e:
        print(e)
    finally:
        db.close_conn()

    # dtale.show(pd.DataFrame([1,2,3,4,5]), subprocess=False)
    #main()
    #createSchema()
    #experiment()
    #addColumnToSchema()
    #experiment()

    # Nice to have TODOS
    # - we could map FeatureNormalizers with table creation, since they use the same field names, we would just need to add type info to FNs and maybe name the class better
    pass