import logging
from typing import Any, Dict, List, Optional
import os
from dotenv import load_dotenv
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
from pydantic import AnyUrl
import json
import argparse
from sentence_transformers import SentenceTransformer

# Import HubSpotClient and ApiException from our module
from .hubspot_client import HubSpotClient, ApiException
# Import FAISS manager
from .faiss_manager import FaissManager
# Import utility functions
from .utils import store_in_faiss, search_in_faiss

logger = logging.getLogger('mcp_hubspot_server')

async def main(access_token: Optional[str] = None):
    """Run the HubSpot MCP server."""
    logger.info("Server starting")
    
    # Initialize FAISS manager
    storage_dir = os.getenv("HUBSPOT_STORAGE_DIR", "/storage")
    logger.info(f"Using storage directory: {storage_dir}")
    
    # Load the embedding model at startup
    logger.info("Loading embeddings model")
    # Try to use local model if exists, otherwise download from HuggingFace
    local_model_path = '/app/models/all-MiniLM-L6-v2'
    if os.path.exists(local_model_path):
        logger.info(f"Using local model from {local_model_path}")
        embedding_model = SentenceTransformer(local_model_path)
    else:
        logger.info("Local model not found, downloading from HuggingFace")
        embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
    
    # Get the model's embedding dimension
    embedding_dim = embedding_model.get_sentence_embedding_dimension()
    logger.info(f"Embeddings model loaded with dimension: {embedding_dim}")
    
    # Create FAISS manager with correct embedding dimension
    faiss_manager = FaissManager(
        storage_dir=storage_dir,
        embedding_dimension=embedding_dim
    )
    logger.info(f"FAISS manager initialized with dimension {embedding_dim}")
    
    # Initialize HubSpot client
    hubspot = HubSpotClient(access_token)
    server = Server("hubspot-manager")

    @server.list_resources()
    async def handle_list_resources() -> list[types.Resource]:
        return []

    @server.read_resource()
    async def handle_read_resource(uri: AnyUrl) -> str:
        if uri.scheme != "hubspot":
            raise ValueError(f"Unsupported URI scheme: {uri.scheme}")

        path = str(uri).replace("hubspot://", "")
        return ""

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        """List available tools"""
        return [
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
                name="hubspot_get_active_companies",
                description="Get most recently active companies from HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "description": "Maximum number of companies to return (default: 10)"}
                    },
                },
            ),
            types.Tool(
                name="hubspot_get_active_contacts",
                description="Get most recently active contacts from HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "description": "Maximum number of contacts to return (default: 10)"}
                    },
                },
            ),
            # Add new FAISS search tool
            types.Tool(
                name="hubspot_search_data",
                description="Search for similar data in stored HubSpot API responses",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Text query to search for"},
                        "limit": {"type": "integer", "description": "Maximum number of results to return (default: 10)"}
                    },
                    "required": ["query"]
                },
            ),
            types.Tool(
                name="hubspot_get_recent_conversations",
                description="Get recent conversation threads from HubSpot with their messages",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "description": "Maximum number of threads to return (default: 10)"},
                        "after": {"type": "string", "description": "Pagination token"},
                        "refresh_cache": {"type": "boolean", "description": "Whether to refresh the threads cache (default: false)"}
                    },
                },
            ),
        ]

    @server.call_tool()
    async def handle_call_tool(
        name: str, arguments: dict[str, Any] | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        """Handle tool execution requests"""
        try:
            if name == "hubspot_create_contact":
                if not arguments:
                    raise ValueError("Missing arguments for create_contact")
                
                firstname = arguments["firstname"]
                lastname = arguments["lastname"]
                company = arguments.get("properties", {}).get("company")
                
                # Search for existing contacts with same name and company
                try:
                    from hubspot.crm.contacts import PublicObjectSearchRequest
                    
                    filter_groups = [{
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
                    
                    # Add company filter if provided
                    if company:
                        filter_groups[0]["filters"].append({
                            "propertyName": "company",
                            "operator": "EQ",
                            "value": company
                        })
                    
                    search_request = PublicObjectSearchRequest(
                        filter_groups=filter_groups
                    )
                    
                    search_response = hubspot.client.crm.contacts.search_api.do_search(
                        public_object_search_request=search_request
                    )
                    
                    if search_response.total > 0:
                        # Contact already exists
                        return [types.TextContent(
                            type="text", 
                            text=f"Contact already exists: {search_response.results[0].to_dict()}"
                        )]
                    
                    # If no existing contact found, proceed with creation
                    properties = {
                        "firstname": firstname,
                        "lastname": lastname
                    }
                    
                    # Add email if provided
                    if "email" in arguments:
                        properties["email"] = arguments["email"]
                    
                    # Add any additional properties
                    if "properties" in arguments:
                        properties.update(arguments["properties"])
                    
                    # Create contact using SimplePublicObjectInputForCreate
                    from hubspot.crm.contacts import SimplePublicObjectInputForCreate
                    
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
                
                # Search for existing companies with same name
                try:
                    from hubspot.crm.companies import PublicObjectSearchRequest
                    
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
                        # Company already exists
                        return [types.TextContent(
                            type="text", 
                            text=f"Company already exists: {search_response.results[0].to_dict()}"
                        )]
                    
                    # If no existing company found, proceed with creation
                    properties = {
                        "name": company_name
                    }
                    
                    # Add any additional properties
                    if "properties" in arguments:
                        properties.update(arguments["properties"])
                    
                    # Create company using SimplePublicObjectInputForCreate
                    from hubspot.crm.companies import SimplePublicObjectInputForCreate
                    
                    simple_public_object_input = SimplePublicObjectInputForCreate(
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
                
                # Store in FAISS for future reference
                try:
                    data = json.loads(results)
                    metadata_extras = {"company_id": arguments["company_id"]}
                    logger.debug(f"Preparing to store {len(data) if isinstance(data, list) else 'single'} company_activity data item(s) in FAISS")
                    logger.debug(f"Metadata extras: {metadata_extras}")
                    store_in_faiss(
                        faiss_manager=faiss_manager,
                        data=data,
                        data_type="company_activity",
                        model=embedding_model,
                        metadata_extras=metadata_extras
                    )
                    # Save indexes after successful storage
                    logger.debug("FAISS storage completed, now saving today's index")
                    faiss_manager.save_today_index()
                    logger.debug("Index saving completed")
                except Exception as e:
                    logger.error(f"Error storing in FAISS: {str(e)}", exc_info=True)
                
                return [types.TextContent(type="text", text=results)]
                
            elif name == "hubspot_get_recent_conversations":
                # Extract parameters with defaults if not provided
                limit = arguments.get("limit", 10) if arguments else 10
                after = arguments.get("after") if arguments else None
                refresh_cache = arguments.get("refresh_cache", False) if arguments else False
                
                # Ensure limit is an integer
                limit = int(limit) if limit is not None else 10
                
                # Get recent conversations with pagination
                logger.debug(f"Getting recent conversations with limit={limit}, after={after}, refresh_cache={refresh_cache}")
                results = hubspot.get_recent_conversations(limit=limit, after=after, refresh_cache=refresh_cache)
                
                # Store in FAISS for future reference
                try:
                    data = results.get("results", [])
                    if data:
                        # Store each thread individually in FAISS
                        logger.debug(f"Preparing to store {len(data)} conversation threads in FAISS individually")
                        for i, thread in enumerate(data):
                            thread_metadata = {
                                "thread_id": thread.get("id", f"unknown_{i}"),
                                "limit": limit,
                                "after": after
                            }
                            logger.debug(f"Storing thread {i+1}/{len(data)} with ID {thread_metadata['thread_id']}")
                            
                            # Store single thread as a list with one item to maintain format compatibility
                            store_in_faiss(
                                faiss_manager=faiss_manager,
                                data=[thread],  # Store as single-item list
                                data_type="conversation_thread",
                                model=embedding_model,
                                metadata_extras=thread_metadata
                            )
                        
                        # Save indexes after successful storage
                        logger.debug(f"All {len(data)} threads stored in FAISS, now saving today's index")
                        faiss_manager.save_today_index()
                        logger.debug("Index saving completed")
                except Exception as e:
                    logger.error(f"Error storing conversations in FAISS: {str(e)}", exc_info=True)
                
                # Truncate message text for API response (while preserving full text in FAISS)
                truncated_results = results.copy()
                for thread in truncated_results.get("results", []):
                    for message in thread.get("messages", []):
                        if "text" in message:
                            message["text"] = message["text"][:200] if message["text"] else ""
                        if "rich_text" in message:
                            message["rich_text"] = message["rich_text"][:200] if message["rich_text"] else ""
                
                # Return truncated results as JSON
                return [types.TextContent(type="text", text=json.dumps(truncated_results))]

            elif name == "hubspot_get_active_companies":
                # Extract parameters with defaults if not provided
                limit = arguments.get("limit", 10) if arguments else 10
                
                # Ensure limit is an integer
                limit = int(limit) if limit is not None else 10
                
                # Get recent companies
                results = hubspot.get_recent_companies(limit=limit)
                
                # Store in FAISS for future reference
                try:
                    data = json.loads(results)
                    metadata_extras = {"limit": limit}
                    logger.debug(f"Preparing to store {len(data) if isinstance(data, list) else 'single'} company data item(s) in FAISS")
                    logger.debug(f"Metadata extras: {metadata_extras}")
                    store_in_faiss(
                        faiss_manager=faiss_manager,
                        data=data,
                        data_type="company",
                        model=embedding_model,
                        metadata_extras=metadata_extras
                    )
                    # Save indexes after successful storage
                    logger.debug("FAISS storage completed, now saving today's index")
                    faiss_manager.save_today_index()
                    logger.debug("Index saving completed")
                except Exception as e:
                    logger.error(f"Error storing in FAISS: {str(e)}", exc_info=True)
                
                return [types.TextContent(type="text", text=results)]

            elif name == "hubspot_get_active_contacts":
                # Extract parameters with defaults if not provided
                limit = arguments.get("limit", 10) if arguments else 10
                
                # Ensure limit is an integer
                limit = int(limit) if limit is not None else 10
                
                # Get recent contacts
                results = hubspot.get_recent_contacts(limit=limit)
                
                # Store in FAISS for future reference
                try:
                    data = json.loads(results)
                    metadata_extras = {"limit": limit}
                    logger.debug(f"Preparing to store {len(data) if isinstance(data, list) else 'single'} contact data item(s) in FAISS")
                    logger.debug(f"Metadata extras: {metadata_extras}")
                    store_in_faiss(
                        faiss_manager=faiss_manager,
                        data=data,
                        data_type="contact",
                        model=embedding_model,
                        metadata_extras=metadata_extras
                    )
                    # Save indexes after successful storage
                    logger.debug("FAISS storage completed, now saving today's index")
                    faiss_manager.save_today_index()
                    logger.debug("Index saving completed")
                except Exception as e:
                    logger.error(f"Error storing in FAISS: {str(e)}", exc_info=True)
                
                return [types.TextContent(type="text", text=results)]
                
            elif name == "hubspot_search_data":
                # Extract parameters
                if not arguments or "query" not in arguments:
                    raise ValueError("Missing query parameter for search")
                
                query = arguments["query"]
                limit = arguments.get("limit", 10)
                limit = int(limit) if limit is not None else 10
                
                try:
                    results, _ = search_in_faiss(
                        faiss_manager=faiss_manager,
                        query=query,
                        model=embedding_model,
                        limit=limit
                    )
                    
                    return [types.TextContent(type="text", text=json.dumps(results))]
                except Exception as e:
                    logger.error(f"Error searching in FAISS: {str(e)}")
                    return [types.TextContent(type="text", text=f"Error searching data: {str(e)}")]

            else:
                raise ValueError(f"Unknown tool: {name}")

        except ApiException as e:
            return [types.TextContent(type="text", text=f"HubSpot API error: {str(e)}")]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error: {str(e)}")]

    # Register shutdown handler to save indexes
    import atexit
    atexit.register(faiss_manager.save_today_index)

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
    import argparse
    
    # Set up command line argument parser
    parser = argparse.ArgumentParser(description="Run the HubSpot MCP server")
    parser.add_argument("--access-token", 
                        help="HubSpot API access token (overrides HUBSPOT_ACCESS_TOKEN environment variable)")
    
    args = parser.parse_args()
    asyncio.run(main(access_token=args.access_token)) 