from typing import Dict
from mcp.types import TextContent, Resource, Tool
from pydantic import AnyUrl
from services.deals import DealsService

def register_deals_routes(server, deals_service: DealsService):
    # Add deals resource
    @server.list_resources()
    async def handle_list_resources() -> list[Resource]:
        return [
            Resource(
                uri=AnyUrl("hubspot://hubspot_deals"),
                name="HubSpot Deals",
                description="List of HubSpot deals",
                mimeType="application/json",
            )
        ]

    # Add deals tools
    @server.list_tools()
    async def handle_list_tools() -> list[Tool]:
        return [
            Tool(
                name="hubspot_get_deals",
                description="Get all deals from HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            Tool(
                name="hubspot_get_deals_by_pipeline",
                description="Get deals from a specific pipeline",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "pipeline_id": {"type": "string", "description": "HubSpot pipeline ID"}
                    },
                    "required": ["pipeline_id"]
                },
            ),
            Tool(
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
            Tool(
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
            )
        ]

    # Handle tool calls
    @server.call_tool()
    async def handle_deals_tools(name: str, arguments: Dict | None) -> list[TextContent]:
        if name == "hubspot_get_deals":
            results = deals_service.get_all_deals()
            return [TextContent(type="text", text=str(results))]

        elif name == "hubspot_get_deals_by_pipeline":
            if not arguments:
                raise ValueError("Missing arguments for get_deals_by_pipeline")
            results = deals_service.get_deals_by_pipeline(arguments["pipeline_id"])
            return [TextContent(type="text", text=results)]

        elif name == "hubspot_create_deal":
            if not arguments:
                raise ValueError("Missing arguments for create_deal")
            results = deals_service.create_deal(arguments["properties"])
            return [TextContent(type="text", text=results)]

        elif name == "hubspot_update_deal":
            if not arguments:
                raise ValueError("Missing arguments for update_deal")
            results = deals_service.update_deal(arguments["deal_id"], arguments["properties"])
            return [TextContent(type="text", text=results)]

        return []