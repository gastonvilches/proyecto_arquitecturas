import requests
import json
import time
import os
import csv
from datetime import datetime, timedelta


timeframe = '86400' # change also timedelta(hours=1) if timeframe is changed
url = 'https://www.bitstamp.net/api/v2/ohlc/btcusd'
max_requests = 5000
csv_file = 'btcusd_data.csv'
header = ['timestamp', 'open', 'high', 'low', 'close', 'volume']


def date2unix(datetime):
    return str(int(time.mktime(datetime.timetuple())))

if os.path.exists(csv_file):
    raise Exception('CSV file already exist')

# Get data
end_time = date2unix(datetime.now())
params = {'end':end_time, 'step':timeframe, 'limit':'1000'}
r = requests.get(url, params=params)
data_json = json.loads(r.content)

# Print start and end timestamps
start_time = data_json['data']['ohlc'][0]['timestamp']
end_time = data_json['data']['ohlc'][-1]['timestamp']
print(datetime.utcfromtimestamp(int(start_time)), '-',
      datetime.utcfromtimestamp(int(end_time)), '-',
          len(data_json['data']['ohlc']), 'lines')

# Write to csv
with open(csv_file, 'a', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=header)
    writer.writeheader()
    data_json['data']['ohlc'].reverse()
    writer.writerows(data_json['data']['ohlc'])
    data_json['data']['ohlc'].reverse()

num_requests = 1
while num_requests < max_requests:
    # Get next start and end timestamps
    end_time = datetime.fromtimestamp(int(data_json['data']['ohlc'][0]['timestamp']))
    end_time = date2unix(end_time - timedelta(days=1))

    # Get data
    params = {'end':end_time, 'step':timeframe, 'limit':'1000'}
    r = requests.get(url, params=params)
    data_json = json.loads(r.content)
    if not len(data_json['data']['ohlc']) > 0:
        break
    
    # Write to csv
    with open(csv_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        data_json['data']['ohlc'].reverse()
        writer.writerows(data_json['data']['ohlc'])
        data_json['data']['ohlc'].reverse()
    
    # Print start and end timestamps
    start_time = data_json['data']['ohlc'][0]['timestamp']
    end_time = data_json['data']['ohlc'][-1]['timestamp']
    print(datetime.utcfromtimestamp(int(start_time)), '-',
          datetime.utcfromtimestamp(int(end_time)), '-',
          len(data_json['data']['ohlc']), 'lines')
    
    num_requests += 1


