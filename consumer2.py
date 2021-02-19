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
    if (not valid_trip_event(data)):
        return False
    if (not valid_direction(data)):
        return False
    if (not valid_route_number(data)):
        return False

    return True

# Starting with these at least
def valid_trip_event(data):
    if(data["TRIP_ID"] < 140000000):
        return False
    return True

def valid_direction(data):
    if(data["DIRECTION"] == None
       or data["DIRECTION"] == ""
       or data["DIRECTION"] == " " 
       or int(data["DIRECTION"]) < 0
       or int(data["DIRECTION"]) > 1):
        return False
    return True

def valid_route_number(data):
    if(int(data["ROUTE_NUMBER"]) < 0 or int(data["ROUTE_NUMBER"]) > 2000):
        return False
    return True

def convert_data(entry):
    direction = entry["DIRECTION"]
    if (direction == '0'):
        entry["DIRECTION"] = "Out"
    else:
        entry["DIRECTION"] = "Back"

# Update appropriate entries in database
def write_to_db(data, conn):
    with conn.cursor() as cursor:
        cursor.execute("""
        UPDATE Trip 
        SET
           route_id = %s,
           direction = %s
        WHERE
           trip_id = %s
        ;""", (data["ROUTE_NUMBER"], data["DIRECTION"], data["TRIP_ID"]))

    return

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    config_file = "/.confluence/librdkafka.config"
    topic = "project_topic_2"
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
                data = json.loads(record_value)
                if (data_is_valid(data)):
                    convert_data(data)
                    write_to_db(data, connection)

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        connection.close()
