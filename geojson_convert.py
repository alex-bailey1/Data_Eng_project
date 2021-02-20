#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point

DBname = "postgres"
DBuser = ""
DBpwd = ""

def write_from_db(conn):
    connection = psycopg2.connect(
        host="35.233.168.90",
        database=DBname,
        user=DBuser,
        password=DBpwd,
	)
    connection.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute("""SELECT latitude, longitude, velocity FROM BreadCrumb;""")

features = []
with open('sample_data.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    data = csvfile.readlines()
    for line in data[1:]:
        row = line.split("\t")
        
        # Uncomment these lines
        # lat = row[Index of latitude column]
        # long = row[Index of longitude column]
        # speed = row[Index of velocity column]

        # skip the rows where speed is missing
        if speed is None or speed == "":
            continue
     	
        try:
            latitude, longitude = map(float, (lat, long))
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

collection = FeatureCollection(features)
with open("data.geojson", "w") as f:
    f.write('%s' % collection)

