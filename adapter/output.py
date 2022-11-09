import sqlite3
from sqlite3 import Error
from datetime import datetime
import json

class OutputRepository:
    conn = None

    def __init__(self) -> None:
        pass

    def open_db_conn(self):
        pass

    def close_conn(self):
        pass

    def insert_to_database(self, flat):
        pass

    def alreadyPresentDays(self):
        return []

    def db_exists(self):
        pass

    def createSchema(self, FEATURE_NORMALIZERS):
        pass

class LoggingRepository:
    conn = None

    def __init__(self) -> None:
        pass

    def open_db_conn(self):
        pass

    def close_conn(self):
        pass

    def insert_to_database(self, to_insert):
        print("################ Logging to insert:\n\n")
        print(json.dumps(to_insert,default=str))
        pass

    def alreadyPresentDays(self):
        return []

    def db_exists(self):
        pass

    def createSchema(self, FEATURE_NORMALIZERS):
        pass

class SqlLiteRepository(OutputRepository):
    conn = None
    db_path = None

    def __init__(self, db_path) -> None:
        self.db_path = db_path

    def open_db_conn(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
        except Error as e:
            print(e)

    def close_conn(self):
        if self.conn:
            self.conn.close()

    def insert_to_database(self, to_insert):
        cur = self.conn.cursor()
        for myDict in to_insert:
            columns = ', '.join(myDict.keys())
            placeholders = ', '.join('?' * len(myDict))
            sql = 'INSERT INTO autos ({}) VALUES ({})'.format(columns, placeholders)
            values = [x for x in myDict.values()]
            cur.execute(sql,values)
        self.conn.commit()

    def alreadyPresentDays(self):
        assert self.conn != None

        query = """
        select DISTINCT(scan_date) from autos;
        """
        try:
            cur = self.conn.cursor()
            cur.execute(query)

            rows = cur.fetchall()
            already_present_dates = [datetime.fromtimestamp(row[0]) for row in rows]
            return already_present_dates
        except Error as e:
            print("Error getting already present days")
            print(e)
            return []

    def db_exists(self):
        assert self.conn != None

        query = """
        select DISTINCT(scan_date) from autos;
        """
        try:
            cur = self.conn.cursor()
            cur.execute(query)

            rows = cur.fetchall()
            return True
        except Error as e:
            print(e)
            return False

    ### You need to modify the table if introducing additional features
    def createSchema(self, FEATURE_NORMALIZERS):
        # TODO: maybe we can move that to the db class and only provide the list of feature fields as input
        assert self.conn != None

        create_table_str = """
            CREATE TABLE "autos" (
                'index' INTEGER PRIMARY KEY AUTOINCREMENT,
                'manufacturer' TEXT,
                'modelgroup' TEXT,
                'model' TEXT,
                'cashprice_raw' INTEGER,
                'thumbnail' TEXT,
                'vin' TEXT,
                'parking_place' TEXT,
                'location' TEXT,
                'tag' TEXT,
                'id' INTEGER,
                'kw' INTEGER,
                'date_order' TIMESTAMP,
                'licensedate' TIMESTAMP,
                'mileage_km' INTEGER,
                'scan_date' TIMESTAMP,
                'internal_vehicle_number' INTEGER,
                'equipment_level' TEXT,
                'last_maintenance_km' REAL,
                'missed' TEXT
        """
        create_index_str = 'CREATE INDEX "ix_autos_index"ON "autos" ("index")'

        for fn in FEATURE_NORMALIZERS:
            create_table_str += ",'" + fn.FIELD + "'" + " TEXT\n"

        create_table_str += ")"
        print(create_table_str)

        self.conn.execute(create_table_str)
        self.conn.execute(create_index_str)