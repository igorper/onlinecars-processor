import json
from datetime import datetime
import os
import sys

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
from logger import LoggerFramework

FILTER_ENDPOINT_PATH = '/api/v1/car/filter'

global logger
logger = LoggerFramework.configure_logger("save_current_day_json")


def crawl_and_extract_filter_json(logs, out_file):
    lookup = {}
    for entry in logs:
        log = json.loads(entry["message"])["message"]
        if (
            "Network.response" in log["method"]
            or "Network.request" in log["method"]
            or "Network.webSocket" in log["method"]
        ) and "requestId" in log['params'] and "headers" in log['params'] and ":path" in log['params']['headers'] and log['params']['headers'][':path'] == FILTER_ENDPOINT_PATH:
            lookup[log['params']['requestId']] = log['params']['headers'][':path']

    logger.debug("XHR lookup size is: %d", len(lookup))

    items = {}
    for entry in logs:
        log = json.loads(entry["message"])["message"]
        if ("Network.responseReceived" in log["method"] and "params" in log.keys() and "headers" in log['params'] and "content-type" in log['params']['headers'] and "application/json" == log['params']['headers']['content-type'] and log['params']['requestId'] in lookup):
            body = driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': log["params"]["requestId"]})
            # print(body['body'])
            j = json.loads(body['body'])
            logger.debug("Extracted %d items for %s", len(j['data']['cars']), FILTER_ENDPOINT_PATH)
            for c in j['data']['cars']:
                if 'internal-vehicle-number' in c.keys() and c['internal-vehicle-number'] not in items.keys():
                    items[c['internal-vehicle-number']] = c

    final_object = {}
    final_object['data'] = {}
    final_object['data']['cars'] = list(items.values())

    logger.debug("Cleaning up and merging partial results")
    logger.info("Number of items extracted: %d", len(final_object['data']['cars']))
    
    json_object = json.dumps(final_object)

    with open(out_file, "w") as outfile:
        logger.debug("Writing results to %s", out_file)
        outfile.write(json_object)


# use chrome performance logs to capture xhr requests
def set_capabilities_to_capture_xhr():
    capabilities = DesiredCapabilities.CHROME
    capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
    return capabilities

# schedule with crontab
# `0 16-23 * * * /bin/bash ~/home-code/onlinecars-processor/save_current_day_all_data.sh all > /tmp/cronstd.log 2>/tmp/cronerr.log`
if __name__ == "__main__":
    logger.info("Started new run for args %s", sys.argv)

    url = "https://www.onlinecars.at/search?manufacturers=VW,Mercedes,BMW&models=Golf,Passat&constructions=Kombi&price=3000,23000&year=2017,2022&mileage=0,150000&"
    output_folder = "input/"
    sleep_time = 5

    if len(sys.argv) > 1:
        logger.debug("Running for all cars")
        url = "https://www.onlinecars.at/search"
        output_folder = "all_cars_input/"
        sleep_time = 60
    else:
        logger.debug("Running for prefiltered cars")

    today = datetime.today().strftime('%d-%m-%Y')
    out_file = output_folder + today +  ".json"

    logger.debug("Checking if file exits: %s", out_file)

    if os.path.exists(out_file):
        logger.info("%s exists. Already done for today", out_file)
        exit(0)

    driver = None
    try:
        capabilities = set_capabilities_to_capture_xhr() 

        driver = webdriver. Chrome(service=Service(ChromeDriverManager().install()), desired_capabilities=capabilities)
        driver.minimize_window()

        logger.debug("Using url: %s", url)

        driver.get(url)
        # TODO: we could save a screenshot every day to make sure we are collecting correct data
        # driver.save_screenshot('scr.png')
        logger.debug("Sleeping for %d seconds", sleep_time)
        sleep(sleep_time)

        logs = driver.get_log("performance")
        crawl_and_extract_filter_json(logs, out_file)
    finally:
        if driver is not None:
            driver.close()
            driver.quit()