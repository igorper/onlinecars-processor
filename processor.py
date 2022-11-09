from sqlite3 import Error
from logic.car_mapper import CarMapper
from logic.feature_normalizer import FeatureNormalizer
from adapter.output import OutputRepository, SqlLiteRepository
from adapter.input import InputRepository, DiskInputRepository

DB_PATH = "data/all_database_reorg.db"
INPUT_PATH = "input"
FEATURE_NORMALIZERS = FeatureNormalizer.parse_from_json()


def main(input_repo: InputRepository, output_repo: OutputRepository):
    already_scanned = output_repo.alreadyPresentDays()

    all_files_without_scanned = input_repo.items_to_process(already_scanned)

    car_mapper = CarMapper()

    print("Files left for scanning (count: " + str(len(all_files_without_scanned)) + ")")
    # print(all_files_without_scanned)

    all_to_insert = []
    for file_name in all_files_without_scanned:
        mapped_auto = car_mapper.process_date_file(FEATURE_NORMALIZERS, file_name)

        if mapped_auto is not None:
            all_to_insert.append(mapped_auto)

    for item in all_to_insert:
        try:
            output_repo.insert_to_database(item)
        except Error as e:
            print("Error inserting a new entry")
            print(e)


if __name__ == "__main__":
    debug_flag = True

    # db: DataRepository = LoggingRepository()
    # input: InputRepository = DebugInputRepository()

    db: OutputRepository = SqlLiteRepository(DB_PATH)
    input: InputRepository = DiskInputRepository(INPUT_PATH)

    db.open_db_conn()

    try:
        db_exists = db.db_exists()
        if not db_exists:
            print("DB doesn't exist yet, initializing it first")
            print("Creating table ...")
            db.createSchema(FEATURE_NORMALIZERS)

        main(input, db)
    except Error as e:
        print(e)
    finally:
        db.close_conn()

    # - a script that would fill-in feature mapping based on some distance -> e.g levenstein

    pass
