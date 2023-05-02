import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from influxdb import InfluxDBClient

# InfluxDB credentials
INFLUXDB_HOST = "3.87.199.82"
INFLUXDB_PORT = 8086
INFLUXDB_USERNAME = "admin"
INFLUXDB_PASSWORD = "password"
INFLUXDB_DATABASE = "currency_rates"

# Get XML data
response = requests.get("https://www.bnr.ro/nbrfxrates.xml")
root = ET.fromstring(response.content)

# Extract data and send to InfluxDB
client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD, database=INFLUXDB_DATABASE)





for cube in root.iter("Cube"):
    if "date" in cube.attrib:
        # Get date
        date_str = cube.attrib["date"]
        date = datetime.strptime(date_str, "%Y-%m-%d")

        # Insert exchange rates for the date
        for rate in cube.iter("Rate"):
            json_body = [
                {
                    "measurement": "exchange_rates",
                    "tags": {
                        "currency": rate.attrib["currency"]
                    },
                    "time": date,
                    "fields": {
                        "rate": float(rate.attrib["rate"]),
                        "currency_name": rate.attrib["currencyname"]
                    }
                }
            ]
            client.write_points(json_body)
result = client.query('SHOW MEASUREMENTS')
easurements = [m['name'] for m in result.get_points()]
print(measurements)
print("Data sent to InfluxDB successfully!")
