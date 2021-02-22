#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point
import psycopg2

DBname = "postgres"
DBuser = ""
DBpwd = ""

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
        # print("data:")
        # print(cursor.fetchall())
        return cursor.fetchall()



def convert_to_geojson(data):
    features = []
    for line in data:        
        # Uncomment these lines
        lat = line[0]
        long_val = line[1]
        speed = line[2]

        # skip the rows where speed is missing
        if speed is None or speed == "":
            continue
     	
        try:
            latitude, longitude = map(float, (lat, long_val))
            #print(latitude)
            #print(longitude)
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
    #print(features)

# features = []
# with open('sample_data.tsv', newline='') as csvfile:
#     reader = csv.reader(csvfile, delimiter='\t')
#     data = csvfile.readlines()
#     for line in data[1:]:
#         row = line.split("\t")
        
#         # Uncomment these lines
#         # lat = row[Index of latitude column]
#         # long = row[Index of longitude column]
#         # speed = row[Index of velocity column]

#         # skip the rows where speed is missing
#         if speed is None or speed == "":
#             continue
     	
#         try:
#             latitude, longitude = map(float, (lat, long))
#             features.append(
#                 Feature(
#                     geometry = Point((longitude,latitude)),
#                     properties = {
#                         'speed': (int(speed))
#                     }
#                 )
#             )
#         except ValueError:
#             continue

# collection = FeatureCollection(features)
# with open("data.geojson", "w") as f:
#     f.write('%s' % collection)


data = write_from_db()

convert_to_geojson(data)
