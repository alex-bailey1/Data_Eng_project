from confluent_kafka import Consumer
import json
import sys
sys.path.append("/examples/clients/cloud/python/")
import ccloud_lib
# import pandas as pd
import math
import datetime

def data_is_valid(data):
    # Check to see if the record passes all the checks. Return false if it doesn't
    if (not valid_event_no_trip):
        return False
    elif (not valid_event_no_trip_range):
        return False
    elif (not valid_act_time_range(data)):
        return False
    elif (not valid_latitude(data)):
        return False
    elif (not valid_direction(data)):
        return False
    elif (not valid_vehicle_id(data)):
        return False
    elif (not gps_longitude_has_corresponding_lattitude(data)):
        return False
    elif (not valid_velocity(data)):
        return False

    return True

# Velocity must be between 0 and 60 inclusive
def valid_velocity(data):
    if (data["VELOCITY"] < 0 or data["VELOCITY"] > 60):
        return False
    return True

# Latitude must be between 45 and 47 inclusive
def valid_latitude(data):
    if (data["GPS_LATITUDE"] < 45 or data["GPS_LATITUDE"] > 47):
        return False
    return True

# direction cannot be lower than 0 and higher than 360 inclusive
def valid_direction(data):
    if (data["DIRECTION"] < 0 or data["DIRECTION"] > 360):
        return False
    return True

# very tuple must have an EVENT_NO_TRIP
# need to test
def valid_event_no_trip(data):
    if (data["EVENT_NO_TRIP"] is None or data["EVENT_NO_TRIP"] == "" or data["EVENT_NO_TRIP"] == 0):
        return False
    return True

# Every EVENT_NO_TRIP should be above 140000000
def valid_event_no_trip_range(data):
    if(data["EVENT_NO_TRIP"] < 140000000):
        return False
    return True


# Every tuple must have a VEHICLE_ID
def valid_vehicle_id(data):
    if (data["VEHICLE_ID"] is None or data["VEHICLE_ID"] == "" or data["VEHICLE_ID"] == 0):
        return False
    return True

# ACT_TIME should be in the range 0-93600 (0-26 hours)
def valid_act_time_range(data):
    if(data["ACT_TIME"] < 0 or data["ACT_TIME"] > 93600):
        return False
    return True

# Every GPS_LATITUDE must have a corresponding GPS_LONGITUDE
def gps_longitude_has_corresponding_lattitude(data):
    if ((data["GPS_LATITUDE"] is not None and data["GPS_LATITUDE"] != 0) and
        (data["GPS_LONGITUDE"] is None or data["GPS_LONGITUDE"] == 0)):
        return False
    return True

# Write entry to database
def write_to_db(data):
    # Split the data up for uploading into Postgre
    # Must insert
    bread_crumb_values = (
       data["ACT_TIME"],
       data["GPS_LATTITUDE"],
       data["GPS_LONGITUDE"],
       data["DIRECTION"],
       data["VELOCITY"],
       data["EVENT_NO_TRIP"] 
    )

    # SELECT trip_id from Trip WHERE trip_id = (%s), (data["EVENT_NO_TRIP"])
    # Try inserting. Ignore if it fails
    trip_values = (
        data["EVENT_NO_TRIP"],
        0,
        data["VEHICLE_ID"],
        datetime.datetime(data["OPD_DATE"]).weekday(),
        data["DIRECTION"]
    )



    return

total_count = 0
num_acceptable = 0
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
                    total_count = total_count + 1
                    if (data["VELOCITY"] >= 5 and data["VELOCITY"] <= 15):
                        num_acceptable = num_acceptable + 1
                    if (total_count < 1000 or (num_acceptable/total_count) >= 0.4):
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
