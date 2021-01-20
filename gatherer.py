# Python Data Gatherer - Python3
# For Data Engineering Project Assignment1 Part B (and later, E)
# 1/13/2021

# Import
from datetime import datetime
from urllib import request
import json

# Get data and write it to a file
date = datetime.today().strftime("%Y-%m-%d")
with open(date + '.json', 'w') as output_file:
    request = request.urlopen('http://rbi.ddns.net/getBreadCrumbData')
    the_parse = json.load(request)
    output_file.write(request.read().decode('utf-8'))

# References (I may have been a little paranoid about citing here):
# https://stackoverflow.com/questions/32490629/getting-todays-date-in-yyyy-mm-dd-in-python
# https://stackoverflow.com/questions/1369526/what-is-the-python-keyword-with-used-for
# https://stackoverflow.com/questions/12092527/python-write-bytes-to-file
# https://stackoverflow.com/questions/57278599/python-write-json-file-from-url-python-3-adding-n-and-b
# https://stackoverflow.com/questions/23131227/how-to-readlines-from-urllib
# https://stackoverflow.com/questions/606191/convert-bytes-to-a-string
# https://docs.python.org/3/library/http.client.html
# https://docs.python.org/3/library/urllib.request.html#module-urllib.request
