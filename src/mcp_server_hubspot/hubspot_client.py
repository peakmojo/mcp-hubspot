import logging
import os
import json
import requests
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from dateutil.tz import tzlocal
import pathlib

from hubspot import HubSpot
from hubspot.crm.contacts import SimplePublicObjectInputForCreate
from hubspot.crm.contacts.exceptions import ApiException

# Re-export ApiException
__all__ = ["HubSpotClient", "ApiException"]

logger = logging.getLogger('mcp_hubspot_client')

def convert_datetime_fields(obj: Any) -> Any:
    """Convert any datetime or tzlocal objects to string in the given object"""
    if isinstance(obj, dict):
        return {k: convert_datetime_fields(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_fields(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, tzlocal):
        # Get the current timezone offset
        offset = datetime.now(tzlocal()).strftime('%z')
        return f"UTC{offset[:3]}:{offset[3:]}"  # Format like "UTC+08:00" or "UTC-05:00"
    return obj

class HubSpotClient:
    def __init__(self, access_token: Optional[str] = None):
        access_token = access_token or os.getenv("HUBSPOT_ACCESS_TOKEN")
        logger.debug(f"Using access token: {'[MASKED]' if access_token else 'None'}")
        if not access_token:
            raise ValueError("HUBSPOT_ACCESS_TOKEN environment variable is required")
        
        self.client = HubSpot(access_token=access_token)
        self.storage_dir = pathlib.Path("storage")
        self.storage_dir.mkdir(exist_ok=True)
        self.threads_file = self.storage_dir / "conversation_threads.json"
        self.threads_cache = self._load_threads_cache()

    def _load_threads_cache(self) -> Dict[str, Any]:
        """Load conversation threads from cache file if it exists"""
        try:
            if self.threads_file.exists():
                with open(self.threads_file, "r") as f:
                    return json.load(f)
            return {"results": [], "paging": {"next": {"after": None}}}
        except Exception as e:
            logger.error(f"Error loading threads cache: {str(e)}")
            return {"results": [], "paging": {"next": {"after": None}}}

    def _save_threads_cache(self, threads_data: Dict[str, Any]) -> None:
        """Save conversation threads to cache file"""
        try:
            with open(self.threads_file, "w") as f:
                json.dump(threads_data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving threads cache: {str(e)}")

    def get_recent_companies(self, limit: int = 10) -> str:
        """Get most recently active companies from HubSpot
        
        Args:
            limit: Maximum number of companies to return (default: 10)
        """
        try:
            from hubspot.crm.companies import PublicObjectSearchRequest
            
            # Create search request with sort by lastmodifieddate
            search_request = PublicObjectSearchRequest(
                sorts=[{
                    "propertyName": "lastmodifieddate",
                    "direction": "DESCENDING"
                }],
                limit=limit,
                properties=["name", "domain", "website", "phone", "industry", "hs_lastmodifieddate"]
            )
            
            # Execute the search
            search_response = self.client.crm.companies.search_api.do_search(
                public_object_search_request=search_request
            )
            
            # Convert the response to a dictionary
            companies_dict = [company.to_dict() for company in search_response.results]
            converted_companies = convert_datetime_fields(companies_dict)
            return json.dumps(converted_companies)
            
        except ApiException as e:
            logger.error(f"API Exception: {str(e)}")
            return json.dumps({"error": str(e)})
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            return json.dumps({"error": str(e)})

    def get_recent_contacts(self, limit: int = 10) -> str:
        """Get most recently active contacts from HubSpot
        
        Args:
            limit: Maximum number of contacts to return (default: 10)
        """
        try:
            from hubspot.crm.contacts import PublicObjectSearchRequest
            
            # Create search request with sort by lastmodifieddate
            search_request = PublicObjectSearchRequest(
                sorts=[{
                    "propertyName": "lastmodifieddate",
                    "direction": "DESCENDING"
                }],
                limit=limit,
                properties=["firstname", "lastname", "email", "phone", "company", "hs_lastmodifieddate", "lastmodifieddate"]
            )
            
            # Execute the search
            search_response = self.client.crm.contacts.search_api.do_search(
                public_object_search_request=search_request
            )
            
            # Convert the response to a dictionary
            contacts_dict = [contact.to_dict() for contact in search_response.results]
            converted_contacts = convert_datetime_fields(contacts_dict)
            return json.dumps(converted_contacts)
            
        except ApiException as e:
            logger.error(f"API Exception: {str(e)}")
            return json.dumps({"error": str(e)})
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            return json.dumps({"error": str(e)})

    def get_company_activity(self, company_id: str) -> str:
        """Get activity history for a specific company"""
        try:
            # Step 1: Get all engagement IDs associated with the company using CRM Associations v4 API
            associated_engagements = self.client.crm.associations.v4.basic_api.get_page(
                object_type="companies",
                object_id=company_id,
                to_object_type="engagements",
                limit=500
            )
            
            # Extract engagement IDs from the associations response
            engagement_ids = []
            if hasattr(associated_engagements, 'results'):
                for result in associated_engagements.results:
                    engagement_ids.append(result.to_object_id)

            # Step 2: Get detailed information for each engagement
            activities = []
            for engagement_id in engagement_ids:
                engagement_response = self.client.api_request({
                    "method": "GET",
                    "path": f"/engagements/v1/engagements/{engagement_id}"
                }).json()
                
                engagement_data = engagement_response.get('engagement', {})
                metadata = engagement_response.get('metadata', {})
                
                # Format the engagement
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
                
                # Add type-specific metadata formatting
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

            # Convert any datetime fields and return
            converted_activities = convert_datetime_fields(activities)
            return json.dumps(converted_activities)
            
        except ApiException as e:
            logger.error(f"API Exception: {str(e)}")
            return json.dumps({"error": str(e)})
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            return json.dumps({"error": str(e)})

    def get_recent_emails(self, limit: int = 10, after: Optional[str] = None) -> Dict[str, Any]:
        """Get recent emails from HubSpot with pagination
        
        Args:
            limit: Maximum number of emails to return per page (default: 10)
            after: Pagination token from a previous call (default: None)
            
        Returns:
            Dictionary containing email data and pagination token
        """
        try:
            # Get a page of emails
            logger.debug(f"Fetching {limit} emails with after={after}")
            api_response = self.client.crm.objects.emails.basic_api.get_page(
                limit=limit, 
                archived=False,
                after=after
            )
            
            # Extract email IDs to fetch their bodies in batch
            email_ids = [email.id for email in api_response.results]
            logger.debug(f"Found {len(email_ids)} email IDs")
            
            if not email_ids:
                logger.info("No emails found")
                return {
                    "results": [],
                    "pagination": {
                        "next": {"after": api_response.paging.next.after if hasattr(api_response, 'paging') and hasattr(api_response.paging, 'next') else None}
                    }
                }
            
            # Get detailed body content for each email
            formatted_emails = []
            
            # Process emails in batches of 10 (HubSpot API limit for batch operations)
            batch_size = 10
            for i in range(0, len(email_ids), batch_size):
                batch_ids = email_ids[i:i+batch_size]
                logger.debug(f"Processing batch of {len(batch_ids)} emails")
                
                try:
                    # Make batch API request for email details
                    from hubspot.crm.objects.emails import BatchReadInputSimplePublicObjectId, SimplePublicObjectId
                    
                    batch_input = BatchReadInputSimplePublicObjectId(
                        inputs=[SimplePublicObjectId(id=email_id) for email_id in batch_ids],
                        properties=["subject", "hs_email_text", "hs_email_html", "hs_email_from", "hs_email_to", "hs_email_cc", "hs_email_bcc", "createdAt", "updatedAt"]
                    )
                    
                    batch_response = self.client.crm.objects.emails.batch_api.read(
                        batch_read_input_simple_public_object_id=batch_input
                    )
                    
                    # Format each email response
                    for email in batch_response.results:
                        email_dict = email.to_dict()
                        properties = email_dict.get("properties", {})
                        
                        formatted_email = {
                            "id": email_dict.get("id"),
                            "created_at": properties.get("createdAt"),
                            "updated_at": properties.get("updatedAt"),
                            "subject": properties.get("subject", ""),
                            "from": properties.get("hs_email_from", ""),
                            "to": properties.get("hs_email_to", ""),
                            "cc": properties.get("hs_email_cc", ""),
                            "bcc": properties.get("hs_email_bcc", ""),
                            "body": properties.get("hs_email_text", "") or properties.get("hs_email_html", "")
                        }
                        
                        formatted_emails.append(formatted_email)
                        
                except ApiException as e:
                    logger.error(f"Batch API Exception: {str(e)}")
                    
            # Convert datetime fields
            converted_emails = convert_datetime_fields(formatted_emails)
            
            # Get pagination token for the next page
            next_after = api_response.paging.next.after if hasattr(api_response, 'paging') and hasattr(api_response.paging, 'next') else None
            
            return {
                "results": converted_emails,
                "pagination": {
                    "next": {"after": next_after}
                }
            }
            
        except ApiException as e:
            logger.error(f"API Exception: {str(e)}")
            return {"error": str(e), "results": [], "pagination": {"next": {"after": None}}}
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            return {"error": str(e), "results": [], "pagination": {"next": {"after": None}}}

    def get_recent_conversations(self, limit: int = 10, after: Optional[str] = None, refresh_cache: bool = False) -> Dict[str, Any]:
        """Get recent conversation threads from HubSpot with pagination
        
        Args:
            limit: Maximum number of threads to return per page (default: 10)
            after: Pagination token from a previous call (default: None)
            refresh_cache: Whether to refresh the threads cache (default: False)
            
        Returns:
            Dictionary containing conversation threads with their messages and pagination token
        """
        try:
            # Use cached threads unless refresh_cache is True or we're paginating
            if not refresh_cache and not after and self.threads_cache.get("results"):
                logger.info("Using cached threads")
                threads_data = self.threads_cache
            else:
                # Get a page of threads
                logger.debug(f"Fetching {limit} threads with after={after}")
                url = "https://api.hubapi.com/conversations/v3/conversations/threads"
                
                params = {"limit": limit, "sort": "-id"}
                if after:
                    params["after"] = after
                
                headers = {
                    'accept': "application/json",
                    'authorization': f"Bearer {self.client.access_token}"
                }
                
                response = requests.request("GET", url, headers=headers, params=params)
                threads_response = response.json()
                
                # Save or update threads cache
                if not after:  # Only replace full cache when getting first page
                    self._save_threads_cache(threads_response)
                    self.threads_cache = threads_response
                
                threads_data = threads_response
            
            thread_results = threads_data.get("results", [])
            logger.debug(f"Found {len(thread_results)} threads")
            
            if not thread_results:
                logger.info("No threads found")
                return {
                    "results": [],
                    "pagination": {
                        "next": {"after": threads_data.get("paging", {}).get("next", {}).get("after")}
                    }
                }
            
            # Get messages for each thread
            formatted_threads = []
            
            for thread in thread_results:
                thread_id = thread.get("id")
                if not thread_id:
                    continue
                
                # Get the last 2 messages for this thread
                try:
                    url = f"https://api.hubapi.com/conversations/v3/conversations/threads/{thread_id}/messages"
                    params = {}  
                    
                    headers = {
                        'accept': "application/json",
                        'authorization': f"Bearer {self.client.access_token}"
                    }
                    
                    messages_response = requests.request("GET", url, headers=headers, params=params).json()
                    
                    # Format thread with its messages
                    message_results = messages_response.get("results", [])
                    
                    # Only keep actual messages (not system messages)
                    actual_messages = [msg for msg in message_results if msg.get("type") == "MESSAGE"]
                    
                    formatted_thread = {
                        "id": thread_id,
                        "created_at": thread.get("createdAt"),
                        "status": thread.get("status"),
                        "inbox_id": thread.get("inboxId"),
                        "associated_contact_id": thread.get("associatedContactId"),
                        "spam": thread.get("spam", False),
                        "archived": thread.get("archived", False),
                        "assigned_to": thread.get("assignedTo"),
                        "latest_message_timestamp": thread.get("latestMessageTimestamp"),
                        "messages": []
                    }
                    
                    # Add formatted messages
                    for msg in actual_messages:
                        sender_info = {}
                        if msg.get("senders") and len(msg.get("senders")) > 0:
                            sender = msg.get("senders")[0]
                            sender_info = {
                                "actor_id": sender.get("actorId", ""),
                                "name": sender.get("name", ""),
                                "sender_field": sender.get("senderField", ""),
                                "email": sender.get("deliveryIdentifier", {}).get("value", "") if sender.get("deliveryIdentifier", {}).get("type") == "HS_EMAIL_ADDRESS" else ""
                            }
                        
                        recipients_info = []
                        for recipient in msg.get("recipients", []):
                            if recipient.get("deliveryIdentifier", {}).get("type") == "HS_EMAIL_ADDRESS":
                                recipients_info.append({
                                    "recipient_field": recipient.get("recipientField", ""),
                                    "email": recipient.get("deliveryIdentifier", {}).get("value", "")
                                })
                        
                        formatted_message = {
                            "id": msg.get("id"),
                            "created_at": msg.get("createdAt"),
                            "updated_at": msg.get("updatedAt"),
                            "sender": sender_info,
                            "recipients": recipients_info,
                            "subject": msg.get("subject", ""),
                            "text": msg.get("text", ""),
                            "rich_text": msg.get("richText", ""),
                            "status": msg.get("status", {}).get("statusType", ""),
                            "direction": msg.get("direction", ""),
                            "channel_id": msg.get("channelId", ""),
                            "channel_account_id": msg.get("channelAccountId", "")
                        }
                        
                        formatted_thread["messages"].append(formatted_message)
                    
                    formatted_threads.append(formatted_thread)
                    
                except Exception as e:
                    logger.error(f"Error fetching messages for thread {thread_id}: {str(e)}")
            
            # Convert datetime fields
            converted_threads = convert_datetime_fields(formatted_threads)
            
            # Get pagination token for the next page
            next_after = threads_data.get("paging", {}).get("next", {}).get("after")
            
            return {
                "results": converted_threads,
                "pagination": {
                    "next": {"after": next_after}
                }
            }
            
        except ApiException as e:
            logger.error(f"API Exception: {str(e)}")
            return {"error": str(e), "results": [], "pagination": {"next": {"after": None}}}
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            return {"error": str(e), "results": [], "pagination": {"next": {"after": None}}} 