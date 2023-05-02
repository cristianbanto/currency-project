import requests
from influxdb import InfluxDBClient
import xml.etree.ElementTree as ET
import datetime
import pandas as pd

# Define the endpoint URL to fetch the XML data
url = 'https://www.bnr.ro/nbrfxrates.xml'

# Fetch the XML data from the endpoint
response = requests.get(url)
xml_data = response.text

# Parse the XML data
root = ET.fromstring(xml_data)
cubes = root.findall('.//Cube')

# Extract currency rates and dates
data = []
for cube in cubes:
    currency = cube.attrib.get('currency')
    rate = float(cube.text)
    date = datetime.datetime.now().strftime('%Y-%m-%d')
    data.append({'currency': currency, 'rate': rate, 'date': date})

# Convert data to DataFrame
df = pd.DataFrame(data)

# Connect to the InfluxDB database
influxdb_host = 'http://3.87.199.82'
influxdb_port = 8086
influxdb_database = 'currency_rates'
influxdb_client = InfluxDBClient(host=influxdb_host, port=influxdb_port)
influxdb_client.switch_database(influxdb_database)

# Convert DataFrame to InfluxDB Line Protocol format
data_points = []
for _, row in df.iterrows():
    data_points.append({
        'measurement': 'currency_rate',
        'tags': {'currency': row['currency']},
        'time': row['date'],
        'fields': {'rate': row['rate']}
    })

# Write data points to InfluxDB
influxdb_client.write_points(data_points)

# Close the InfluxDB connection
influxdb_client.close()