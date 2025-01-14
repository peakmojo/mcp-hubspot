import logging
from typing import Any, Dict, List, Optional
import os
from dotenv import load_dotenv
from hubspot import HubSpot
from hubspot.crm.contacts import SimplePublicObjectInputForCreate
from hubspot.crm.contacts.exceptions import ApiException
from hubspot.crm.contacts import PublicObjectSearchRequest
from hubspot.crm.companies import SimplePublicObjectInputForCreate as CompanyInputForCreate
from hubspot.crm.deals import SimplePublicObjectInputForCreate as DealInputForCreate
from hubspot.crm.deals import PublicObjectSearchRequest as DealSearchRequest
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
from pydantic import AnyUrl
import json
from datetime import datetime, timedelta
from dateutil.tz import tzlocal

logger = logging.getLogger('mcp_hubspot_server')

def convert_datetime_fields(obj: Any) -> Any:
    """Convert any datetime or tzlocal objects to string in the given object"""
    if isinstance(obj, dict):
        return {k: convert_datetime_fields(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_fields(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, tzlocal):
        offset = datetime.now(tzlocal()).strftime('%z')
        return f"UTC{offset[:3]}:{offset[3:]}"
    return obj

class HubSpotClient:
    def __init__(self, access_token: Optional[str] = None):
        access_token = access_token or os.getenv("HUBSPOT_ACCESS_TOKEN")
        logger.debug(f"Using access token: {'[MASKED]' if access_token else 'None'}")
        if not access_token:
            raise ValueError("HUBSPOT_ACCESS_TOKEN environment variable is required")
        
        self.client = HubSpot(access_token=access_token)
        self.DEFAULT_LIMIT = 10
        self.MAX_RESPONSE_LENGTH = 1048576

    def _create_pagination_info(self, total: int, current_page: int, has_more: bool = False) -> dict:
        """Create standardized pagination metadata"""
        return {
            "pagination": {
                "total_records": total,
                "current_page": current_page,
                "page_size": self.DEFAULT_LIMIT,
                "total_pages": (total + self.DEFAULT_LIMIT - 1) // self.DEFAULT_LIMIT,
                "has_next_page": has_more,
                "has_previous_page": current_page > 1
            }
        }


    def get_contacts(self, page: int = 1) -> str:
        """Get contacts from HubSpot with pagination"""
        try:
            after = (page - 1) * self.DEFAULT_LIMIT if page > 1 else None
            contacts = self.client.crm.contacts.basic_api.get_page(
                limit=self.DEFAULT_LIMIT,
                after=after,
                properties=['firstname', 'lastname', 'email', 'company']
            )
            contacts_dict = [contact.to_dict() for contact in contacts.results]
            converted_contacts = convert_datetime_fields(contacts_dict)
            
            response = {
                "results": converted_contacts,
                **self._create_pagination_info(
                    total=contacts.total,
                    current_page=page,
                    has_more=contacts.paging.next.after if contacts.paging and contacts.paging.next else False
                )
            }
            return json.dumps(self._limit_response_size(response))
        except ApiException as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})


    def get_companies(self, page: int = 1) -> str:
        """Get companies from HubSpot with pagination"""
        try:
            after = (page - 1) * self.DEFAULT_LIMIT if page > 1 else None
            companies = self.client.crm.companies.basic_api.get_page(
                limit=self.DEFAULT_LIMIT,
                after=after,
                properties=['name', 'domain', 'industry']
            )
            companies_dict = [company.to_dict() for company in companies.results]
            converted_companies = convert_datetime_fields(companies_dict)
            
            response = {
                "results": converted_companies,
                **self._create_pagination_info(
                    total=companies.total,
                    current_page=page,
                    has_more=companies.paging.next.after if companies.paging and companies.paging.next else False
                )
            }
            return json.dumps(self._limit_response_size(response))
        except ApiException as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})


    def get_company_activity(self, company_id: str) -> str:
        """Get activity history for a specific company"""
        try:
            associated_engagements = self.client.crm.associations.v4.basic_api.get_page(
                object_type="companies",
                object_id=company_id,
                to_object_type="engagements",
                limit=500
            )
            
            engagement_ids = []
            if hasattr(associated_engagements, 'results'):
                for result in associated_engagements.results:
                    engagement_ids.append(result.to_object_id)

            activities = []
            for engagement_id in engagement_ids:
                engagement_response = self.client.api_request({
                    "method": "GET",
                    "path": f"/engagements/v1/engagements/{engagement_id}"
                }).json()
                
                engagement_data = engagement_response.get('engagement', {})
                metadata = engagement_response.get('metadata', {})
                
                formatted_engagement = {
                    "id": engagement_data.get("id"),
                    "type": engagement_data.get("type"),
                    "created_at": engagement_data.get("createdAt"),
                    "last_updated": engagement_data.get("lastUpdated"),
                    "created_by": engagement_data.get("createdBy"),
                    "modified_by": engagement_data.get("modifiedBy"),
                    "timestamp": engagement_data.get("timestamp"),
                    "associations": engagement_response.get("associations", {})
                }
                
                if engagement_data.get("type") == "NOTE":
                    formatted_engagement["content"] = metadata.get("body", "")
                elif engagement_data.get("type") == "EMAIL":
                    formatted_engagement["content"] = {
                        "subject": metadata.get("subject", ""),
                        "from": {
                            "raw": metadata.get("from", {}).get("raw", ""),
                            "email": metadata.get("from", {}).get("email", ""),
                            "firstName": metadata.get("from", {}).get("firstName", ""),
                            "lastName": metadata.get("from", {}).get("lastName", "")
                        },
                        "to": [{
                            "raw": recipient.get("raw", ""),
                            "email": recipient.get("email", ""),
                            "firstName": recipient.get("firstName", ""),
                            "lastName": recipient.get("lastName", "")
                        } for recipient in metadata.get("to", [])],
                        "cc": [{
                            "raw": recipient.get("raw", ""),
                            "email": recipient.get("email", ""),
                            "firstName": recipient.get("firstName", ""),
                            "lastName": recipient.get("lastName", "")
                        } for recipient in metadata.get("cc", [])],
                        "bcc": [{
                            "raw": recipient.get("raw", ""),
                            "email": recipient.get("email", ""),
                            "firstName": recipient.get("firstName", ""),
                            "lastName": recipient.get("lastName", "")
                        } for recipient in metadata.get("bcc", [])],
                        "sender": {
                            "email": metadata.get("sender", {}).get("email", "")
                        },
                        "body": metadata.get("text", "") or metadata.get("html", "")
                    }
                elif engagement_data.get("type") == "TASK":
                    formatted_engagement["content"] = {
                        "subject": metadata.get("subject", ""),
                        "body": metadata.get("body", ""),
                        "status": metadata.get("status", ""),
                        "for_object_type": metadata.get("forObjectType", "")
                    }
                elif engagement_data.get("type") == "MEETING":
                    formatted_engagement["content"] = {
                        "title": metadata.get("title", ""),
                        "body": metadata.get("body", ""),
                        "start_time": metadata.get("startTime"),
                        "end_time": metadata.get("endTime"),
                        "internal_notes": metadata.get("internalMeetingNotes", "")
                    }
                elif engagement_data.get("type") == "CALL":
                    formatted_engagement["content"] = {
                        "body": metadata.get("body", ""),
                        "from_number": metadata.get("fromNumber", ""),
                        "to_number": metadata.get("toNumber", ""),
                        "duration_ms": metadata.get("durationMilliseconds"),
                        "status": metadata.get("status", ""),
                        "disposition": metadata.get("disposition", "")
                    }
                
                activities.append(formatted_engagement)

            converted_activities = convert_datetime_fields(activities)
            return json.dumps(converted_activities)
            
        except ApiException as e:
            logger.error(f"API Exception: {str(e)}")
            return json.dumps({"error": str(e)})
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            return json.dumps({"error": str(e)})

    def get_recent_activities(self) -> str:
        """Get recent activities from HubSpot for the last 3 days"""
        try:
            three_days_ago = int((datetime.now() - timedelta(days=3)).timestamp() * 1000)
            
            activities = []
            offset = None
            
            while True:
                params = {
                    "count": 100,
                    "since": three_days_ago
                }
                if offset:
                    params["offset"] = offset
                
                response = self.client.api_request({
                    "method": "GET",
                    "path": "/engagements/v1/engagements/recent/modified",
                    "params": params
                }).json()
                
                if 'results' in response:
                    for result in response['results']:
                        engagement_data = result.get('engagement', {})
                        metadata = result.get('metadata', {})
                        
                        formatted_engagement = {
                            "id": engagement_data.get("id"),
                            "type": engagement_data.get("type"),
                            "created_at": engagement_data.get("createdAt"),
                            "last_updated": engagement_data.get("lastUpdated"),
                            "created_by": engagement_data.get("createdBy"),
                            "modified_by": engagement_data.get("modifiedBy"),
                            "timestamp": engagement_data.get("timestamp"),
                            "associations": result.get("associations", {})
                        }
                        
                        if engagement_data.get("type") == "NOTE":
                            formatted_engagement["content"] = metadata.get("body", "")
                        elif engagement_data.get("type") == "EMAIL":
                            formatted_engagement["content"] = {
                                "subject": metadata.get("subject", ""),
                                "from": {
                                    "raw": metadata.get("from", {}).get("raw", ""),
                                    "email": metadata.get("from", {}).get("email", ""),
                                    "firstName": metadata.get("from", {}).get("firstName", ""),
                                    "lastName": metadata.get("from", {}).get("lastName", "")
                                },
                                "to": [{
                                    "raw": recipient.get("raw", ""),
                                    "email": recipient.get("email", ""),
                                    "firstName": recipient.get("firstName", ""),
                                    "lastName": recipient.get("lastName", "")
                                } for recipient in metadata.get("to", [])],
                                "cc": [{
                                    "raw": recipient.get("raw", ""),
                                    "email": recipient.get("email", ""),
                                    "firstName": recipient.get("firstName", ""),
                                    "lastName": recipient.get("lastName", "")
                                } for recipient in metadata.get("cc", [])],
                                "bcc": [{
                                    "raw": recipient.get("raw", ""),
                                    "email": recipient.get("email", ""),
                                    "firstName": recipient.get("firstName", ""),
                                    "lastName": recipient.get("lastName", "")
                                } for recipient in metadata.get("bcc", [])],
                                "sender": {
                                    "email": metadata.get("sender", {}).get("email", "")
                                },
                                "body": metadata.get("text", "") or metadata.get("html", "")
                            }
                        elif engagement_data.get("type") == "TASK":
                            formatted_engagement["content"] = {
                                "subject": metadata.get("subject", ""),
                                "body": metadata.get("body", ""),
                                "status": metadata.get("status", ""),
                                "for_object_type": metadata.get("forObjectType", "")
                            }
                        elif engagement_data.get("type") == "MEETING":
                            formatted_engagement["content"] = {
                                "title": metadata.get("title", ""),
                                "body": metadata.get("body", ""),
                                "start_time": metadata.get("startTime"),
                                "end_time": metadata.get("endTime"),
                                "internal_notes": metadata.get("internalMeetingNotes", "")
                            }
                        elif engagement_data.get("type") == "CALL":
                            formatted_engagement["content"] = {
                                "body": metadata.get("body", ""),
                                "from_number": metadata.get("fromNumber", ""),
                                "to_number": metadata.get("toNumber", ""),
                                "duration_ms": metadata.get("durationMilliseconds"),
                                "status": metadata.get("status", ""),
                                "disposition": metadata.get("disposition", "")
                            }
                        
                        activities.append(formatted_engagement)
                
                if not response.get('hasMore', False):
                    break
                    
                offset = response.get('offset')
            
            converted_activities = convert_datetime_fields(activities)
            return json.dumps(converted_activities)
            
        except ApiException as e:
            logger.error(f"API Exception: {str(e)}")
            return json.dumps({"error": str(e)})
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            return json.dumps({"error": str(e)})

    # Deals Methods
    def get_all_deals(self, page: int = 1) -> str:
        """Get deals from HubSpot with pagination"""
        try:
            after = (page - 1) * self.DEFAULT_LIMIT if page > 1 else None
            deals = self.client.crm.deals.basic_api.get_page(
                limit=self.DEFAULT_LIMIT,
                after=after,
                properties=['dealname', 'amount', 'dealstage', 'pipeline']
            )
            deals_dict = [deal.to_dict() for deal in deals.results]
            converted_deals = convert_datetime_fields(deals_dict)
            
            response = {
                "results": converted_deals,
                **self._create_pagination_info(
                    total=deals.total,
                    current_page=page,
                    has_more=deals.paging.next.after if deals.paging and deals.paging.next else False
                )
            }
            return json.dumps(self._limit_response_size(response))
        except ApiException as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})


    def get_deals_by_pipeline(self, pipeline_id: str, page: int = 1) -> str:
        """Get deals from a specific pipeline with pagination"""
        try:
            filter_groups = [{
                "filters": [{
                    "propertyName": "pipeline",
                    "operator": "EQ",
                    "value": pipeline_id
                }]
            }]
            
            search_request = DealSearchRequest(
                filter_groups=filter_groups,
                limit=self.DEFAULT_LIMIT,
                after=(page - 1) * self.DEFAULT_LIMIT if page > 1 else None,
                properties=['dealname', 'amount', 'dealstage']
            )
            
            deals = self.client.crm.deals.search_api.do_search(
                public_object_search_request=search_request
            )
            deals_dict = [deal.to_dict() for deal in deals.results]
            converted_deals = convert_datetime_fields(deals_dict)
            
            response = {
                "results": converted_deals,
                **self._create_pagination_info(
                    total=deals.total,
                    current_page=page,
                    has_more=deals.paging.next.after if deals.paging and deals.paging.next else False
                )
            }
            return json.dumps(self._limit_response_size(response))
        except ApiException as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})

            
    def create_deal(self, properties: Dict) -> str:
        """Create a new deal in HubSpot"""
        try:
            simple_public_object_input = DealInputForCreate(
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
        """Update an existing deal in HubSpot"""
        try:
            simple_public_object_input = DealInputForCreate(
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

    # Lists Methods
    def get_contact_lists(self, page: int = 1) -> str:
        """Get contact lists from HubSpot with pagination"""
        try:
            offset = (page - 1) * self.DEFAULT_LIMIT
            response = self.client.api_request({
                "method": "GET",
                "path": "/contacts/v1/lists",
                "params": {
                    "count": self.DEFAULT_LIMIT,
                    "offset": offset,
                    "properties": ["name", "listId", "dynamic"]
                }
            })
            lists_data = response.json()
            
            response = {
                "results": lists_data.get("lists", [])[:self.DEFAULT_LIMIT],
                **self._create_pagination_info(
                    total=lists_data.get("total", 0),
                    current_page=page,
                    has_more=lists_data.get("has-more", False)
                )
            }
            return json.dumps(self._limit_response_size(response))
        except Exception as e:
            return json.dumps({"error": str(e)})


    def create_contact_list(self, name: str, dynamic: bool, filters: List[Dict]) -> str:
        """Create a new contact list in HubSpot"""
        try:
            data = {
                "name": name,
                "dynamic": dynamic,
                "filters": filters
            }
            
            response = self.client.api_request({
                "method": "POST",
                "path": "/contacts/v1/lists",
                "json": data
            })
            return json.dumps(response.json())
        except Exception as e:
            return json.dumps({"error": str(e)})

    def get_list_contacts(self, list_id: str, page: int = 1) -> str:
        """Get contacts in a specific list with pagination"""
        try:
            vid_offset = (page - 1) * self.DEFAULT_LIMIT
            response = self.client.api_request({
                "method": "GET",
                "path": f"/contacts/v1/lists/{list_id}/contacts/all",
                "params": {
                    "count": self.DEFAULT_LIMIT,
                    "vidOffset": vid_offset,
                    "property": ["firstname", "lastname", "email"]
                }
            })
            contacts_data = response.json()
            
            response = {
                "results": contacts_data.get("contacts", [])[:self.DEFAULT_LIMIT],
                **self._create_pagination_info(
                    total=contacts_data.get("total", 0),
                    current_page=page,
                    has_more="vid-offset" in contacts_data.get("has-more", {})
                )
            }
            return json.dumps(self._limit_response_size(response))
        except Exception as e:
            return json.dumps({"error": str(e)})


    # Workflows Methods
    def get_workflows(self) -> str:
        """Get all workflows from HubSpot"""
        try:
            response = self.client.api_request({
                "method": "GET",
                "path": "/automation/v3/workflows"
            })
            workflows_data = response.json()
            converted_data = convert_datetime_fields(workflows_data)
            return json.dumps(converted_data)
        except Exception as e:
            return json.dumps({"error": str(e)})

    def get_workflow_by_id(self, workflow_id: str) -> str:
        """Get a specific workflow by ID"""
        try:
            response = self.client.api_request({
                "method": "GET",
                "path": f"/automation/v3/workflows/{workflow_id}"
            })
            workflow_data = response.json()
            converted_data = convert_datetime_fields(workflow_data)
            return json.dumps(converted_data)
        except Exception as e:
            return json.dumps({"error": str(e)})

    def enroll_contact_in_workflow(self, workflow_id: str, email: str) -> str:
        """Enroll a contact in a specific workflow"""
        try:
            data = {
                "email": email
            }
            
            response = self.client.api_request({
                "method": "POST",
                "path": f"/automation/v2/workflows/{workflow_id}/enrollments/contacts",
                "json": data
            })
            return json.dumps(response.json())
        except Exception as e:
            return json.dumps({"error": str(e)})

    # Reports Methods
    def get_analytics_report(self, report_type: str, start_date: str, end_date: str) -> str:
        """Get analytics report from HubSpot"""
        try:
            params = {
                "start": start_date,
                "end": end_date
            }
            
            response = self.client.api_request({
                "method": "GET",
                "path": f"/analytics/v2/reports/{report_type}",
                "params": params
            })
            report_data = response.json()
            converted_data = convert_datetime_fields(report_data)
            return json.dumps(converted_data)
        except Exception as e:
            return json.dumps({"error": str(e)})

    def get_deal_pipeline_report(self) -> str:
        """Get deal pipeline performance report"""
        try:
            response = self.client.api_request({
                "method": "GET",
                "path": "/deals/v1/deal/pipelinestages/pipeline-performance"
            })
            report_data = response.json()
            converted_data = convert_datetime_fields(report_data)
            return json.dumps(converted_data)
        except Exception as e:
            return json.dumps({"error": str(e)})

    def get_custom_report(self, report_definition: Dict) -> str:
        """Get custom report based on provided definition"""
        try:
            response = self.client.api_request({
                "method": "POST",
                "path": "/analytics/v2/reports/custom",
                "json": report_definition
            })
            report_data = response.json()
            converted_data = convert_datetime_fields(report_data)
            return json.dumps(converted_data)
        except Exception as e:
            return json.dumps({"error": str(e)})

async def main(access_token: Optional[str] = None):
    """Run the HubSpot MCP server."""
    logger.info("Server starting")
    hubspot = HubSpotClient(access_token)
    server = Server("hubspot-manager")

    @server.list_resources()
    async def handle_list_resources() -> list[types.Resource]:
        return [
            types.Resource(
                uri=AnyUrl("hubspot://hubspot_contacts"),
                name="HubSpot Contacts",
                description="List of HubSpot contacts",
                mimeType="application/json",
            ),
            types.Resource(
                uri=AnyUrl("hubspot://hubspot_companies"),
                name="HubSpot Companies", 
                description="List of HubSpot companies",
                mimeType="application/json",
            ),
            types.Resource(
                uri=AnyUrl("hubspot://hubspot_recent_engagements"),
                name="HubSpot Recent Engagements",
                description="HubSpot engagements from all companies and contacts from the last 3 days",
                mimeType="application/json",
            ),
            types.Resource(
                uri=AnyUrl("hubspot://hubspot_deals"),
                name="HubSpot Deals",
                description="List of HubSpot deals",
                mimeType="application/json",
            ),
                        types.Resource(
                uri=AnyUrl("hubspot://hubspot_lists"),
                name="HubSpot Lists",
                description="List of HubSpot contact lists",
                mimeType="application/json",
            ),
            types.Resource(
                uri=AnyUrl("hubspot://hubspot_workflows"),
                name="HubSpot Workflows",
                description="List of HubSpot workflows",
                mimeType="application/json",
            ),
            types.Resource(
                uri=AnyUrl("hubspot://hubspot_reports"),
                name="HubSpot Reports",
                description="HubSpot analytics reports",
                mimeType="application/json",
            )
        ]

    @server.read_resource()
    async def handle_read_resource(uri: AnyUrl) -> str:
        if uri.scheme != "hubspot":
            raise ValueError(f"Unsupported URI scheme: {uri.scheme}")

        path = str(uri).replace("hubspot://", "")
        if path == "hubspot_contacts":
            return str(hubspot.get_contacts())
        elif path == "hubspot_companies":
            return str(hubspot.get_companies())
        elif path == "hubspot_recent_engagements":
            return str(hubspot.get_recent_activities())
        elif path == "hubspot_deals":
            return str(hubspot.get_all_deals())
        else:
            raise ValueError(f"Unknown resource path: {path}")

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        """List available tools"""
        return [
            types.Tool(
                name="hubspot_get_contacts",
                description="Get contacts from HubSpot (10 records per page)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "page": {"type": "integer", "description": "Page number (starts from 1)"}
                    }
                },
            ),
            types.Tool(
                name="hubspot_create_contact",
                description="Create a new contact in HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "firstname": {"type": "string", "description": "Contact's first name"},
                        "lastname": {"type": "string", "description": "Contact's last name"},
                        "email": {"type": "string", "description": "Contact's email address"},
                        "properties": {"type": "object", "description": "Additional contact properties"}
                    },
                    "required": ["firstname", "lastname"]
                },
            ),
            types.Tool(
                name="hubspot_get_companies",
                description="Get companies from HubSpot (10 records per page)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "page": {"type": "integer", "description": "Page number (starts from 1)"}
                    }
                },
            ),
            types.Tool(
                name="hubspot_create_company",
                description="Create a new company in HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Company name"},
                        "properties": {"type": "object", "description": "Additional company properties"}
                    },
                    "required": ["name"]
                },
            ),
            types.Tool(
                name="hubspot_get_company_activity",
                description="Get activity history for a specific company",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "company_id": {"type": "string", "description": "HubSpot company ID"}
                    },
                    "required": ["company_id"]
                },
            ),
            types.Tool(
                name="hubspot_get_recent_engagements",
                description="Get HubSpot engagements from all companies and contacts from the last 3 days",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            # Deals Tools
            types.Tool(
                name="hubspot_get_deals",
                description="Get deals from HubSpot (10 records per page)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "page": {"type": "integer", "description": "Page number (starts from 1)"}
                    }
                },
            ),
            types.Tool(
                name="hubspot_get_deals_by_pipeline",
                description="Get deals from a specific pipeline (10 records per page)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "pipeline_id": {"type": "string", "description": "HubSpot pipeline ID"},
                        "page": {"type": "integer", "description": "Page number (starts from 1)"}
                    },
                    "required": ["pipeline_id"]
                },
            ),
            types.Tool(
                name="hubspot_create_deal",
                description="Create a new deal in HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "properties": {
                            "type": "object",
                            "description": "Deal properties",
                            "properties": {
                                "dealname": {"type": "string"},
                                "pipeline": {"type": "string"},
                                "dealstage": {"type": "string"},
                                "amount": {"type": "string"}
                            },
                            "required": ["dealname"]
                        }
                    },
                    "required": ["properties"]
                },
            ),
            types.Tool(
                name="hubspot_update_deal",
                description="Update an existing deal in HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "deal_id": {"type": "string", "description": "HubSpot deal ID"},
                        "properties": {
                            "type": "object",
                            "description": "Deal properties to update"
                        }
                    },
                    "required": ["deal_id", "properties"]
                },
            ),
            types.Tool(
                name="hubspot_get_contact_lists",
                description="Get contact lists from HubSpot (10 records per page)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "page": {"type": "integer", "description": "Page number (starts from 1)"}
                    }
                },
            ),
            types.Tool(
                name="hubspot_create_contact_list",
                description="Create a new contact list in HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "List name"},
                        "dynamic": {"type": "boolean", "description": "Whether the list is dynamic"},
                        "filters": {
                            "type": "array",
                            "description": "List filters",
                            "items": {"type": "object"}
                        }
                    },
                    "required": ["name", "dynamic", "filters"]
                },
            ),
            types.Tool(
                name="hubspot_get_list_contacts",
                description="Get contacts in a specific list (10 records per page)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "list_id": {"type": "string", "description": "HubSpot list ID"},
                        "page": {"type": "integer", "description": "Page number (starts from 1)"}
                    },
                    "required": ["list_id"]
                },
            ),

            # Workflows Tools
            types.Tool(
                name="hubspot_get_workflows",
                description="Get all workflows from HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="hubspot_get_workflow_by_id",
                description="Get a specific workflow by ID",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "workflow_id": {"type": "string", "description": "HubSpot workflow ID"}
                    },
                    "required": ["workflow_id"]
                },
            ),
            types.Tool(
                name="hubspot_enroll_contact_in_workflow",
                description="Enroll a contact in a specific workflow",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "workflow_id": {"type": "string", "description": "HubSpot workflow ID"},
                        "email": {"type": "string", "description": "Contact's email"}
                    },
                    "required": ["workflow_id", "email"]
                },
            ),

            # Reports Tools
            types.Tool(
                name="hubspot_get_analytics_report",
                description="Get analytics report from HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "report_type": {"type": "string", "description": "Type of report"},
                        "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                        "end_date": {"type": "string", "description": "End date (YYYY-MM-DD)"}
                    },
                    "required": ["report_type", "start_date", "end_date"]
                },
            ),
            types.Tool(
                name="hubspot_get_deal_pipeline_report",
                description="Get deal pipeline performance report",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="hubspot_get_custom_report",
                description="Get custom report based on provided definition",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "report_definition": {
                            "type": "object",
                            "description": "Custom report definition"
                        }
                    },
                    "required": ["report_definition"]
                },
            )
        ]

    @server.call_tool()
    async def handle_call_tool(
        name: str, arguments: dict[str, Any] | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        """Handle tool execution requests"""
        try:
            # Get page number from arguments, default to 1 if not provided
            page = arguments.get("page", 1) if arguments else 1
            
            if name == "hubspot_get_contacts":
                results = hubspot.get_contacts(page=page)
                return [types.TextContent(type="text", text=str(results))]

            elif name == "hubspot_get_companies":
                results = hubspot.get_companies(page=page)
                return [types.TextContent(type="text", text=str(results))]

            elif name == "hubspot_get_deals":
                results = hubspot.get_all_deals(page=page)
                return [types.TextContent(type="text", text=str(results))]

            elif name == "hubspot_get_deals_by_pipeline":
                if not arguments:
                    raise ValueError("Missing arguments for get_deals_by_pipeline")
                results = hubspot.get_deals_by_pipeline(
                    pipeline_id=arguments["pipeline_id"],
                    page=page
                )
                return [types.TextContent(type="text", text=results)]

            elif name == "hubspot_get_contact_lists":
                results = hubspot.get_contact_lists(page=page)
                return [types.TextContent(type="text", text=str(results))]

            elif name == "hubspot_get_list_contacts":
                if not arguments:
                    raise ValueError("Missing arguments for get_list_contacts")
                results = hubspot.get_list_contacts(
                    list_id=arguments["list_id"],
                    page=page
                )
                return [types.TextContent(type="text", text=results)]
                
            elif name == "hubspot_create_contact":
                if not arguments:
                    raise ValueError("Missing arguments for create_contact")
                
                firstname = arguments["firstname"]
                lastname = arguments["lastname"]
                company = arguments.get("properties", {}).get("company")
                
                try:
                    search_request = PublicObjectSearchRequest(
                        filter_groups=[{
                            "filters": [
                                {
                                    "propertyName": "firstname",
                                    "operator": "EQ",
                                    "value": firstname
                                },
                                {
                                    "propertyName": "lastname",
                                    "operator": "EQ",
                                    "value": lastname
                                }
                            ]
                        }]
                    )
                    
                    if company:
                        search_request.filter_groups[0]["filters"].append({
                            "propertyName": "company",
                            "operator": "EQ",
                            "value": company
                        })
                    
                    search_response = hubspot.client.crm.contacts.search_api.do_search(
                        public_object_search_request=search_request
                    )
                    
                    if search_response.total > 0:
                        return [types.TextContent(
                            type="text", 
                            text=f"Contact already exists: {search_response.results[0].to_dict()}"
                        )]
                    
                    properties = {
                        "firstname": firstname,
                        "lastname": lastname
                    }
                    
                    if "email" in arguments:
                        properties["email"] = arguments["email"]
                    
                    if "properties" in arguments:
                        properties.update(arguments["properties"])
                    
                    simple_public_object_input = SimplePublicObjectInputForCreate(
                        properties=properties
                    )
                    
                    api_response = hubspot.client.crm.contacts.basic_api.create(
                        simple_public_object_input_for_create=simple_public_object_input
                    )
                    return [types.TextContent(type="text", text=str(api_response.to_dict()))]
                    
                except ApiException as e:
                    return [types.TextContent(type="text", text=f"HubSpot API error: {str(e)}")]

            elif name == "hubspot_create_company":
                if not arguments:
                    raise ValueError("Missing arguments for create_company")
                
                company_name = arguments["name"]
                
                try:
                    search_request = PublicObjectSearchRequest(
                        filter_groups=[{
                            "filters": [
                                {
                                    "propertyName": "name",
                                    "operator": "EQ",
                                    "value": company_name
                                }
                            ]
                        }]
                    )
                    
                    search_response = hubspot.client.crm.companies.search_api.do_search(
                        public_object_search_request=search_request
                    )
                    
                    if search_response.total > 0:
                        return [types.TextContent(
                            type="text", 
                            text=f"Company already exists: {search_response.results[0].to_dict()}"
                        )]
                    
                    properties = {
                        "name": company_name
                    }
                    
                    if "properties" in arguments:
                        properties.update(arguments["properties"])
                    
                    simple_public_object_input = CompanyInputForCreate(
                        properties=properties
                    )
                    
                    api_response = hubspot.client.crm.companies.basic_api.create(
                        simple_public_object_input_for_create=simple_public_object_input
                    )
                    return [types.TextContent(type="text", text=str(api_response.to_dict()))]
                    
                except ApiException as e:
                    return [types.TextContent(type="text", text=f"HubSpot API error: {str(e)}")]

            elif name == "hubspot_get_company_activity":
                if not arguments:
                    raise ValueError("Missing arguments for get_company_activity")
                results = hubspot.get_company_activity(arguments["company_id"])
                return [types.TextContent(type="text", text=results)]

            elif name == "hubspot_get_recent_engagements":
                results = hubspot.get_recent_activities()
                return [types.TextContent(type="text", text=str(results))]

            elif name == "hubspot_create_deal":
                if not arguments:
                    raise ValueError("Missing arguments for create_deal")
                results = hubspot.create_deal(arguments["properties"])
                return [types.TextContent(type="text", text=results)]

            elif name == "hubspot_update_deal":
                if not arguments:
                    raise ValueError("Missing arguments for update_deal")
                results = hubspot.update_deal(arguments["deal_id"], arguments["properties"])
                return [types.TextContent(type="text", text=results)]

            elif name == "hubspot_create_contact_list":
                if not arguments:
                    raise ValueError("Missing arguments for create_contact_list")
                results = hubspot.create_contact_list(
                    arguments["name"],
                    arguments["dynamic"],
                    arguments["filters"]
                )
                return [types.TextContent(type="text", text=results)]

            # Workflows Tool Handlers
            elif name == "hubspot_get_workflows":
                results = hubspot.get_workflows()
                return [types.TextContent(type="text", text=str(results))]

            elif name == "hubspot_get_workflow_by_id":
                if not arguments:
                    raise ValueError("Missing arguments for get_workflow_by_id")
                results = hubspot.get_workflow_by_id(arguments["workflow_id"])
                return [types.TextContent(type="text", text=results)]

            elif name == "hubspot_enroll_contact_in_workflow":
                if not arguments:
                    raise ValueError("Missing arguments for enroll_contact_in_workflow")
                results = hubspot.enroll_contact_in_workflow(
                    arguments["workflow_id"],
                    arguments["email"]
                )
                return [types.TextContent(type="text", text=results)]

            # Reports Tool Handlers
            elif name == "hubspot_get_analytics_report":
                if not arguments:
                    raise ValueError("Missing arguments for get_analytics_report")
                results = hubspot.get_analytics_report(
                    arguments["report_type"],
                    arguments["start_date"],
                    arguments["end_date"]
                )
                return [types.TextContent(type="text", text=results)]

            elif name == "hubspot_get_deal_pipeline_report":
                results = hubspot.get_deal_pipeline_report()
                return [types.TextContent(type="text", text=str(results))]

            elif name == "hubspot_get_custom_report":
                if not arguments:
                    raise ValueError("Missing arguments for get_custom_report")
                results = hubspot.get

            else:
                raise ValueError(f"Unknown tool: {name}")

        except ApiException as e:
            return [types.TextContent(type="text", text=f"HubSpot API error: {str(e)}")]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error: {str(e)}")]

    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        logger.info("Server running with stdio transport")
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="hubspot",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())