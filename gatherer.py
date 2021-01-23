# Python Data Gatherer - Python3
# For Data Engineering Project Assignment1 
# 1/13/2021

# Import
from datetime import datetime, timedelta
from urllib import request
import json
from confluent_kafka import Producer, KafkaError
import sys
sys.path.append("/home/bail34/examples/clients/cloud/python/")
import ccloud_lib

# Get data abd save it
request = request.urlopen('http://rbi.ddns.net/getBreadCrumbData')
the_parse = json.load(request)
date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
with open(date + '.json', 'w') as output_file:
    json.dump(the_parse, output_file)

# Read arguments and configurations and initialize
config_file = "/home/bail34/.confluence/librdkafka.config"
topic = "project_topic"
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
    record_key = "bus_data"
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
