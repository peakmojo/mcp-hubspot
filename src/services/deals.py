from typing import Dict
from hubspot.crm.deals import SimplePublicObjectInputForCreate, PublicObjectSearchRequest
from hubspot.crm.deals.exceptions import ApiException
import json
from datetime import datetime
from utils.datetime_converter import convert_datetime_fields

class DealsService:
    def __init__(self, client):
        self.client = client

    def get_all_deals(self) -> str:
        try:
            deals = self.client.crm.deals.get_all()
            deals_dict = [deal.to_dict() for deal in deals]
            converted_deals = convert_datetime_fields(deals_dict)
            return json.dumps(converted_deals)
        except ApiException as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})

    def get_deals_by_pipeline(self, pipeline_id: str) -> str:
        try:
            filter_groups = [{
                "filters": [{
                    "propertyName": "pipeline",
                    "operator": "EQ",
                    "value": pipeline_id
                }]
            }]
            
            search_request = PublicObjectSearchRequest(
                filter_groups=filter_groups
            )
            
            deals = self.client.crm.deals.search_api.do_search(
                public_object_search_request=search_request
            )
            deals_dict = [deal.to_dict() for deal in deals.results]
            converted_deals = convert_datetime_fields(deals_dict)
            return json.dumps(converted_deals)
        except ApiException as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})

    def create_deal(self, properties: Dict) -> str:
        try:
            simple_public_object_input = SimplePublicObjectInputForCreate(
                properties=properties
            )
            
            deal = self.client.crm.deals.basic_api.create(
                simple_public_object_input_for_create=simple_public_object_input
            )
            return json.dumps(deal.to_dict())
        except ApiException as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})

    def update_deal(self, deal_id: str, properties: Dict) -> str:
        try:
            simple_public_object_input = SimplePublicObjectInputForCreate(
                properties=properties
            )
            
            deal = self.client.crm.deals.basic_api.update(
                deal_id=deal_id,
                simple_public_object_input_for_create=simple_public_object_input
            )
            return json.dumps(deal.to_dict())
        except ApiException as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})