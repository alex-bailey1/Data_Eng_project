from confluent_kafka import Consumer
import json
import sys
sys.path.append("/examples/clients/cloud/python/")
import ccloud_lib
import math
import datetime
import psycopg2
import os

# Database Connection Info
DBname = os.getenv('DB_NAME')
DBuser = os.getenv('DB_USER')
DBpwd = os.getenv('DB_PWD')

def data_is_valid(data):
    # Check to see if the record passes all the checks. Return false if it doesn't
    if (not valid_event_no_trip(data)):
        return False
    elif (not valid_event_no_trip_range(data)):
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

# Velocity must be between 0 and 30 inclusive
def valid_velocity(data):
    if (data["VELOCITY"] < 0 or data["VELOCITY"] > 30):
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
    if(data["ACT_TIME_SECONDS"] < 0 or data["ACT_TIME_SECONDS"] > 93600):
        return False
    return True

# Every GPS_LATITUDE must have a corresponding GPS_LONGITUDE
def gps_longitude_has_corresponding_lattitude(data):
    if ((data["GPS_LATITUDE"] is not None and data["GPS_LATITUDE"] != 0) and
        (data["GPS_LONGITUDE"] is None or data["GPS_LONGITUDE"] == 0)):
        return False
    return True

# Convert json strings to propper types
def convert_data(entry):
    # Check that there are no empty values
    if (entry["EVENT_NO_TRIP"] == ""
        or entry["OPD_DATE"] == ""
        or entry["VEHICLE_ID"] == ""
        or entry["ACT_TIME"] == ""
        or entry["VELOCITY"] == ""
        or entry["DIRECTION"] == ""
        or entry["GPS_LONGITUDE"] == ""
        or entry["GPS_LATITUDE"] == ""):

        # Return something to indicate failure
        return False
    else:
        # Calculate Service Date (day of the week)
        date = datetime.datetime.strptime(entry["OPD_DATE"], "%d-%b-%y")
        day_of_week = date.weekday()
        if (day_of_week < 5):
            day_of_week = "Weekday"
        elif (day_of_week == 5):
            day_of_week = "Saturday"
        else:
            day_of_week = "Sunday"

        # Return a converted form of the data for further processing
        return {
            "EVENT_NO_TRIP": int(entry["EVENT_NO_TRIP"]),
            "OPD_DATE": day_of_week,
            "VEHICLE_ID": int(entry["VEHICLE_ID"]),
            "ACT_TIME": date + datetime.timedelta(seconds=int(entry["ACT_TIME"])),
            "ROUTE_ID": 0,
            "VELOCITY": int(entry["VELOCITY"]),
            "DIRECTION": int(entry["DIRECTION"]),
            "ROUTE_STATUS": "Out",
            "GPS_LONGITUDE": float(entry["GPS_LONGITUDE"]),
            "GPS_LATITUDE": float(entry["GPS_LATITUDE"]),
            "ACT_TIME_SECONDS": int(entry["ACT_TIME"])
        }

# Write entry to database
# Work in progress
def write_to_db(data, conn):
    # Split the data up for uploading into Postgre    
    # SELECT trip_id from Trip WHERE trip_id = (%s), (data["EVENT_NO_TRIP"])
    # Try inserting. Ignore if it fails
    trip_values = {
        'trip_id': data["EVENT_NO_TRIP"],
        'route_id': 0,
        'vehicle_id': data["VEHICLE_ID"],
        'service_key': data["OPD_DATE"],
        'direction': data["ROUTE_STATUS"]
    }

    bread_crumb_values = {
       'tstamp': data["ACT_TIME"],
       'latitude': data["GPS_LATITUDE"],
       'longitude': data["GPS_LONGITUDE"],
       'direction': data["DIRECTION"],
       'speed': data["VELOCITY"],
       'trip_id': data["EVENT_NO_TRIP"] 
    }

    with conn.cursor() as cursor:
        try:
            cursor.execute("INSERT INTO Trip VALUES(%s, %s, %s, %s, %s);", (
                    trip_values["trip_id"],
                    trip_values["route_id"],
                    trip_values["vehicle_id"],
                    trip_values["service_key"],
                    trip_values["direction"]
                )
            )
        except psycopg2.errors.UniqueViolation:
            pass
        
        cursor.execute("INSERT INTO BreadCrumb VALUES(%s, %s, %s, %s, %s, %s) ;", (
                bread_crumb_values["tstamp"],
                bread_crumb_values["latitude"],
                bread_crumb_values["longitude"],
                bread_crumb_values["direction"],
                bread_crumb_values["speed"],
                bread_crumb_values["trip_id"]
            )
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

    # Connect to db
    connection = psycopg2.connect(
        host="35.233.168.90",
        database=DBname,
        user=DBuser,
        password=DBpwd,
	)
    connection.autocommit = True

    # Process messages
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
                data = convert_data(json.loads(record_value))
                if (data != False and data_is_valid(data)):
                    total_count = total_count + 1
                    if (data["VELOCITY"] >= 5 and data["VELOCITY"] <= 15):
                        num_acceptable = num_acceptable + 1
                    if (total_count < 1000 or (num_acceptable/total_count) >= 0.4):
                        write_to_db(data, connection)

                #print(data)
                file1 = open("/home/shengjia/consumer_log.json", "a")

                json.dump(data, file1, default=str)
                file1.close()


    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        connection.close()
