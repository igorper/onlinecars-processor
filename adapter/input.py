from os import walk
from datetime import datetime

class InputRepository:
    def items_to_process(self, already_scanned):
        pass

class DebugInputRepository(InputRepository):

    def items_to_process(self, already_scanned):
        return ['25-06-2022.json']

class DiskInputRepository(InputRepository):
    input_path = None

    def __init__(self, input_path):
        self.input_path = input_path

    def items_to_process(self, already_scanned):
        all_file_names = next(walk(self.input_path), (None, None, []))[2]

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
