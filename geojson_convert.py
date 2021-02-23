#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point
import psycopg2

DBname = "postgres"
DBuser = "database1"
DBpwd = "database1?"

def write_from_db():
    connection = psycopg2.connect(
        host="35.233.168.90",
        database=DBname,
        user=DBuser,
        password=DBpwd,
	)
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute("""SELECT latitude, longitude, speed FROM BreadCrumb LIMIT 10;""")
        return cursor.fetchall()



def convert_to_geojson(data):
    features = []
    for line in data:        
        lat = line[0]
        long_val = line[1]
        speed = line[2]

        # skip the rows where speed is missing
        if speed is None or speed == "":
            continue
     	
        try:
            latitude, longitude = map(float, (lat, long_val))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue
    
    return features



def write_to_file(features):
    collection = FeatureCollection(features)
    with open("data.geojson", "w") as f:
        f.write('%s' % collection)

data = write_from_db()

features = convert_to_geojson(data)

write_to_file(features)
