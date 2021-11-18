import datetime
import time
import Adafruit_DHT 
import csv
from Adafruit_DHT.common import DHT11
import mysql.connector
import json

#Connects to MySQL/MariaDB
db = mysql.connector.connect(host ="localhost",
                     user ="newuser",
                     password = "newuserpassword",
                     db = "RaspberryPi")
#Creates a cursor to pass on demands to MySQL/MariaDB
cur = db.cursor()
# Sensor should be set to Adafruit_DHT.DHT11,
# Adafruit_DHT.DHT22, or Adafruit_DHT.AM2302.
sensor = Adafruit_DHT.DHT11

# Example using a Raspberry Pi with DHT sensor
# connected to GPIO23.
pin = 17


with open(r'DHT11CVS','w') as f: #w means write file
    writer = csv.writer(f)
    writer.writerow(['Date', 'Temperature *C','Humidity %']) # CVS file headers


# Try to grab a sensor reading.  Use the read_retry method which will retry up
# to 15 times to get a sensor reading (waiting 2 seconds between each retry).


# Note that sometimes you won't get a reading and
# the results will be null (because Linux can't
# guarantee the timing of calls to read the sensor).
# If this happens try again!
while True:
    timenow = datetime.datetime.now()
    humidity, temperature = Adafruit_DHT.read_retry(sensor, pin)
    if humidity is not None and temperature is not None:
            print('Temp={0:0.1f}*C  Humidity={1:0.1f}%'.format(temperature, humidity))
    else:
            print('Failed to get reading. Try again!')
    
    #Executes the SQL command in MariaDB to insert data.
    cur.execute('''INSERT INTO DHT11_Data0(date, temperature, humidity) VALUES(%s,%s,%s);''',
                (timenow,temperature, humidity))
    #Commits the data entered above to the table
    db.commit()
    cur.execute('''SELECT id FROM DHT11_Data0''')
    data_table_id= cur.fetchall()
    cur.execute('''SELECT date FROM DHT11_Data0''')
    data_table_date= cur.fetchall()
    cur.execute('''SELECT temperature FROM DHT11_Data0''')
    data_table_temp= cur.fetchall()
    cur.execute('''SELECT humidity FROM DHT11_Data0''')
    data_table_hum= cur.fetchall()

#a means apend ti file
    with open(r'DHT11CVS','a') as f:
        writer = csv.writer(f)
        writer.writerow([timenow,temperature,humidity])
    
    def store_json(data:dict , file_path:str):
        with open (r'Data.json',"w") as json_file:
            json.dump(data,json_file)
    DHT_11 ={}
    DHT_11["ID"] = data_table_id
    DHT_11["Date"] = data_table_date
    DHT_11["Temperature"] = data_table_temp
    DHT_11["Humidity"] = data_table_hum
    
    store_json(DHT_11,r'Data.json')  
    time.sleep(2)