# Python Data Gatherer - Python3
# For Data Engineering Project Assignment3
# 2/18/2021

# Import
from datetime import datetime, timedelta
from urllib import request
import json
from confluent_kafka import Producer, KafkaError
import sys
sys.path.append("/home/bail34/examples/clients/cloud/python/")
import ccloud_lib
from bs4 import BeautifulSoup

# Get html and turn it into json
url = "http://rbi.ddns.net/getStopEvents"
html = request.urlopen(url)
soup = BeautifulSoup(html, 'lxml')
h3s = soup.find_all('h3')
tables = soup.find_all('table')
the_parse = []
for i in range(0, len(h3s)):
    trip_id = int((str(h3s[i]).split(' '))[4]) # https://stackoverflow.com/questions/4289331/how-to-extract-numbers-from-a-string-in-python
    items = tables[i].find_all('tr')[1].find_all('td')
    data = {
        "TRIP_ID": trip_id,
        "VEHICLE_NUMBER": items[0].get_text(),
        "LEAVE_TIME": items[1].get_text(),
        "TRAIN": items[2].get_text(),
        "ROUTE_NUMBER": items[3].get_text(),
        "DIRECTION": items[4].get_text(),
        "SERVICE_KEY": items[5].get_text(),
        "STOP_TIME": items[6].get_text(),
        "ARRIVE_TIME": items[7].get_text(),
        "DWELL": items[8].get_text(),
        "LOCATION_ID": items[9].get_text(),
        "DOOR": items[10].get_text(),
        "LIFT": items[11].get_text(),
        "ONS": items[12].get_text(),
        "OFFS": items[13].get_text(),
        "ESTIMATED_LOAD": items[14].get_text(),
        "MAXIMUM_SPEED": items[15].get_text(),
        "TRAIN_MILEAGE": items[16].get_text(),
        "PATTERN_DISTANCE": items[17].get_text(),
        "LOCATION_DISTANCE": items[18].get_text(),
        "X_COORDINATE": items[19].get_text(),
        "Y_COORDINATE": items[20].get_text(),
        "DATA_SOURCE": items[21].get_text(),
        "SCHEDULE_STATUS": items[22].get_text()
    }
    the_parse.append(data)

date = datetime.today().strftime("%Y-%m-%d")
with open("stop-events-" + date + '.json', 'w') as output_file:
    json.dump(the_parse, output_file)

# Read arguments and configurations and initialize
config_file = "/home/bail34/.confluence/librdkafka.config"
topic = "project_topic_2"
conf = ccloud_lib.read_ccloud_config(config_file)

# Create Producer instance
producer = Producer({
    'bootstrap.servers': conf['bootstrap.servers'],
    'sasl.mechanisms': conf['sasl.mechanisms'],
    'security.protocol': conf['security.protocol'],
    'sasl.username': conf['sasl.username'],
    'sasl.password': conf['sasl.password'],
})

# Create topic if needed
ccloud_lib.create_topic(conf, topic)

# Optional per-message on_delivery handler (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).
def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))

for record in the_parse:
    record_key = "bus_stop_event_data"
    record_value = json.dumps(record)
    producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
    
    producer.poll(0)
    producer.flush()



print("finished")



# References (I may have been a little paranoid about citing here):
# https://stackoverflow.com/questions/32490629/getting-todays-date-in-yyyy-mm-dd-in-python
# https://stackoverflow.com/questions/1369526/what-is-the-python-keyword-with-used-for
# https://stackoverflow.com/questions/12092527/python-write-bytes-to-file
# https://stackoverflow.com/questions/57278599/python-write-json-file-from-url-python-3-adding-n-and-b
# https://stackoverflow.com/questions/23131227/how-to-readlines-from-urllib
# https://stackoverflow.com/questions/606191/convert-bytes-to-a-string
# https://docs.python.org/3/library/http.client.html
# https://docs.python.org/3/library/urllib.request.html#module-urllib.request
# producer code: https://github.com/confluentinc/examples/blob/6.0.1-post/clients/cloud/python/producer.py
# https://www.geeksforgeeks.org/read-write-and-parse-json-using-python/
# https://stackoverflow.com/questions/67631/how-to-import-a-module-given-the-full-path
# https://stackoverflow.com/questions/441147/how-to-subtract-a-day-from-a-date