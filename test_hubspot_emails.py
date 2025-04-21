import os
import hubspot
from pprint import pprint
from hubspot.crm.objects.emails import ApiException

# Get access token from environment variable
access_token = os.environ.get('HUBSPOT_ACCESS_TOKEN')
if not access_token:
    print("Error: HUBSPOT_ACCESS_TOKEN environment variable is not set")
    exit(1)

client = hubspot.Client.create(access_token=access_token)

try:
    api_response = client.crm.objects.emails.basic_api.get_page(limit=2, archived=False)
    pprint(api_response)
except ApiException as e:
    print('Exception when calling basic_api->get_page: %s\n' % e) 