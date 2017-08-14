import boto3
import json
import logging
import os
import requests
import sys
from io import StringIO
from urllib.parse import urlparse

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger('converter')

sess = requests.Session()
auth_tuple = (
    os.environ.get('ACTIONKIT_USERNAME'),
    os.environ.get('ACTIONKIT_PASSWORD')
)
sess.auth = auth_tuple
sess.headers.update({'Accept': 'application/json'})

s3_url = os.environ.get('S3_URL')
s3_parsed = urlparse(s3_url)
assert s3_parsed.scheme == 's3', "Require an s3:// URL to save to"
s3_bucket, s3_key = (s3_parsed.netloc, s3_parsed.path[1:])

campaign_id = int(os.environ.get('ACTIONKIT_CAMPAIGN_ID'))

log.info(
    "Converting events from ActionKit campaign %s to GeoJSON at %s",
    campaign_id, s3_url)

event_geojson = {
    'type': 'FeatureCollection',
    'features': []
}

url_base = 'https://indivisible.actionkit.com'
event_url = url_base + '/rest/v1/event/?campaign={}'.format(campaign_id)
while True:
    resp = sess.get(event_url)

    resp.raise_for_status()
    resp_data = resp.json()

    events = resp_data.get('objects')
    for event in events:
        event_geojson['features'].append({
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [event['longitude'], event['latitude']]
            },
            'properties': {
                'title': event['title'],
                'address': {
                    'address1': event['address1'],
                    'address2': event['address2'],
                    'city': event['city'],
                    'state': event['state'],
                    'zip': event['zip'],
                },
                'time': {
                    'starts_at': event['starts_at_utc'] + 'Z',
                    'ends_at': event['ends_at_utc'] + 'Z' if event['ends_at_utc'] else None,
                }
            }
        })

    event_url = resp_data.get('meta', {}).get('next')
    if not event_url:
        break

    event_url = url_base + event_url

log.info("Found %s events", len(event_geojson['features']))

b = StringIO()
json.dump(event_geojson, b, separators=(',', ':'))

s3 = boto3.resource('s3')
log.info("Writing to bucket %s, key %s", s3_bucket, s3_key)
result = s3.Object(s3_bucket, s3_key).put(
    ACL='public-read',
    ContentType='application/json',
    Body=b.getvalue(),
)
log.info("Result of S3 put: %s", result)
