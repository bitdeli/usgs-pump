import traceback
import time
import os
import json

from cStringIO import StringIO
from csv import DictReader
from urllib import urlopen

MINUTE = 60
USGS_URL = "http://earthquake.usgs.gov/earthquakes/catalogs/eqs1hour-M0.txt"
BITDELI_URL = "https://in.bitdeli.com/events/i-04b9fa914cccfe-34aa9612"
BITDELI_AUTH = os.environ['BITDELI_AUTH']
TIME_FORMAT = "%A, %B %d, %Y %H:%M:%S UTC"

def read_latest():
    try:
        return time.strptime(open('latest-event').read(), TIME_FORMAT)
    except:
        return 0

def write_latest(tstamp):
    f = open('latest-event', 'w')
    f.write(time.strftime(TIME_FORMAT, tstamp))
    f.close()

def send_to_bitdeli(latest_event, parser):
    updated_latest = latest_event
    for entry in parser:
        t = time.strptime(entry['Datetime'], TIME_FORMAT)
        if t > latest_event:
            updated_latest = max(updated_latest, t)
            event = json.dumps({'auth': BITDELI_AUTH, 'object': entry})
            print urlopen(BITDELI_URL, event).read()
    return updated_latest

def pump(latest_event):
    while True:
        try:
            data = urlopen(USGS_URL).read()
            parser = DictReader(StringIO(data))
            latest_event = send_to_bitdeli(latest_event, parser)
            write_latest(latest_event)
        except:
            traceback.print_exc()
        time.sleep(MINUTE)

if __name__ == '__main__':
    pump(read_latest())
