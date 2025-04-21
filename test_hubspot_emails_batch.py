import os
import hubspot
from pprint import pprint
from hubspot.crm.objects.emails import BatchReadInputSimplePublicObjectId, SimplePublicObjectId, ApiException

# Get access token from environment variable
access_token = os.environ.get('HUBSPOT_ACCESS_TOKEN')
if not access_token:
    print("Error: HUBSPOT_ACCESS_TOKEN environment variable is not set")
    exit(1)

client = hubspot.Client.create(access_token=access_token)

# Get email IDs first
try:
    api_response = client.crm.objects.emails.basic_api.get_page(limit=2, archived=False)
    email_ids = [email.id for email in api_response.results]
    print(f"Email IDs: {email_ids}")
    
    # Now get details with batch API
    batch_input = BatchReadInputSimplePublicObjectId(
        inputs=[SimplePublicObjectId(id=email_id) for email_id in email_ids],
        properties=["subject", "hs_email_text", "hs_email_html", "hs_email_from", "hs_email_to", "hs_email_cc", "hs_email_bcc", "createdAt", "updatedAt"]
    )
    
    batch_response = client.crm.objects.emails.batch_api.read(
        batch_read_input_simple_public_object_id=batch_input
    )
    
    print("\nBatch API Response:")
    pprint(batch_response)
    
except ApiException as e:
    print(f'Exception when calling API: {e}\n') 