import json
from datetime import datetime
from unittest.result import failfast

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from time import sleep

def crawl_and_extract_filter_json(logs):
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
    
    json_object = json.dumps(final_object)

    today = datetime.today().strftime('%d-%m-%Y') 
    # Writing to sample.json
    with open("input/" + today + ".json", "w") as outfile:
        outfile.write(json_object)

# parametrize url and output folder
# create cron task that runs every hour (e.g. between 8 and 20)
# should be idempotent to only create a file if it was not yet created on this day
if __name__ == "__main__":

    try:
        chrome_options = Options()
        #chrome_options.add_argument("--headless")
        #chrome_options.add_argument("--window-size=1920x1080")

        capabilities = DesiredCapabilities.CHROME
        capabilities["goog:loggingPrefs"] = {"performance": "ALL"} 
        driver = webdriver. Chrome(chrome_options=chrome_options, executable_path="/opt/homebrew/Caskroom/chromedriver/107.0.5304.62/chromedriver", desired_capabilities=capabilities)
        driver.get("https://www.onlinecars.at/search?manufacturers=VW,Mercedes,BMW&models=Golf,Passat&constructions=Kombi&price=3000,23000&year=2017,2022&mileage=0,150000&")
        sleep(2)

        logs = driver.get_log("performance")

        crawl_and_extract_filter_json(logs)
    finally:    
        driver.close()
        driver.quit()