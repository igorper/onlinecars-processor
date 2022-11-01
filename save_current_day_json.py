import json
from datetime import datetime
from unittest.result import failfast
import os
import sys
import logging

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep

# TODO: toggle to write to a log file, have to separate log files, INFO and DEBUG
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger("save_current_day_json")


def crawl_and_extract_filter_json(logs, out_file):
    lookup = {}
    for entry in logs:
        log = json.loads(entry["message"])["message"]
        if (
            "Network.response" in log["method"]
            or "Network.request" in log["method"]
            or "Network.webSocket" in log["method"]
        ) and "requestId" in log['params'] and "headers" in log['params'] and ":path" in log['params']['headers'] and log['params']['headers'][':path'] == '/api/v1/car/filter':
            lookup[log['params']['requestId']] = log['params']['headers'][':path']

    items = {}
    for entry in logs:
        log = json.loads(entry["message"])["message"]
        if ("Network.responseReceived" in log["method"] and "params" in log.keys() and "headers" in log['params'] and "content-type" in log['params']['headers'] and "application/json" == log['params']['headers']['content-type'] and log['params']['requestId'] in lookup):
            body = driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': log["params"]["requestId"]})
            # print(body['body'])
            j = json.loads(body['body'])
            for c in j['data']['cars']:
                if 'internal-vehicle-number' in c.keys() and c['internal-vehicle-number'] not in items.keys():
                    items[c['internal-vehicle-number']] = c

    final_object = {}
    final_object['data'] = {}
    final_object['data']['cars'] = list(items.values())

    print("Number of items extracted: " + str(len(final_object['data']['cars'])))
    
    json_object = json.dumps(final_object)

    # Writing to sample.json
    with open(out_file, "w") as outfile:
        outfile.write(json_object)


# use chrome performance logs to capture xhr requests
def set_capabilities_to_capture_xhr():
    capabilities = DesiredCapabilities.CHROME
    capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
    return capabilities


if __name__ == "__main__":

    url = "https://www.onlinecars.at/search?manufacturers=VW,Mercedes,BMW&models=Golf,Passat&constructions=Kombi&price=3000,23000&year=2017,2022&mileage=0,150000&"
    output_folder = "test/"
    sleep_time = 5

    if len(sys.argv) > 1:
        print("Running for all cars")
        # url = "https://www.onlinecars.at/search"
        url = "https://www.onlinecars.at/search?manufacturers=VW,Mercedes,BMW&models=Golf,Passat&constructions=Kombi&price=3000,23000&year=2017,2022&mileage=0,150000&"
        output_folder = "all_cars_input/"
        sleep_time = 60
    else:
        print("Running for prefiltered cars")

    today = datetime.today().strftime('%d-%m-%Y')
    out_file = output_folder + today +  ".json"

    if os.path.exists(out_file):
        print("Already done for today")
        exit(0)

    driver = None
    try:
        capabilities = set_capabilities_to_capture_xhr() 

        driver = webdriver. Chrome(service=Service(ChromeDriverManager().install()), desired_capabilities=capabilities)
        driver.minimize_window()
        # TODO: we could save a screenshot every day to make sure we are collecting correct data
        #driver.save_screenshot('path')
        driver.get(url)
        log.info("Sleeping for %d seconds", sleep_time)
        sleep(sleep_time)

        logs = driver.get_log("performance")

        crawl_and_extract_filter_json(logs, out_file)
    finally:
        if driver is not None:
            driver.close()
            driver.quit()