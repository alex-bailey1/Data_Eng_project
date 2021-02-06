from confluent_kafka import Consumer
import json
import sys
sys.path.append("/examples/clients/cloud/python/")
import ccloud_lib
# import pandas as pd
import math
import datetime

def data_is_valid(data):
    if(validate_event_no_trip_range):
        return False
    elif(validate_act_time_range(data)):
        return False
)


# Every EVNET_NO_TRIP should be above 140000000
def validate_event_no_trip_range(data):
        if(data["EVENT_NO_TRIP"] < 140000000):
            return False
        return True


def validate_act_time_range(data):
    if(data["ACT_TIME"] < 0 or data["ACT_TIME"] > 93600):
        return False
    return True

def write_to_db(data):
    return

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    config_file = "/.confluence/librdkafka.config"
    topic = "project_topic"
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'project_group',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    # total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                if(data_is_valid(data)):
                    write_to_db(data)

                #print(data)
                file1 = open("/home/shengjia/consumer_log.json", "a")

                json.dump(data, file1)
                file1.close()


    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
