# HubSpot MCP Server
[![Docker Hub](https://img.shields.io/docker/pulls/buryhuang/mcp-hubspot?label=Docker%20Hub)](https://hub.docker.com/r/buryhuang/mcp-hubspot) 
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

A Model Context Protocol (MCP) server that enables AI assistants to interact with HubSpot CRM data. This server bridges AI models with your HubSpot account, providing direct access to contacts, companies, and engagement data. Built-in vector storage and caching mechanisms help overcome HubSpot API limitations while improving response times.

Our implementation prioritizes the most frequently used, high-value HubSpot operations with robust error handling and API stability. Each component is optimized for AI-friendly interactions, ensuring reliable performance even during complex, multi-step CRM workflows.

## Why MCP-HubSpot?

- **Direct CRM Access**: Connect Claude and other AI assistants to your HubSpot data without intermediary steps
- **Context Retention**: Vector storage with FAISS enables semantic search across previous interactions
- **Zero Configuration**: Simple Docker deployment with minimal setup
- **Efficient Storage**: Dual-database architecture with LevelDB for key-value storage and FAISS for vector search

## Example Prompts

```
Create HubSpot contacts and companies from this LinkedIn profile:
[Paste LinkedIn profile text]
```

```
What's happening lately with my pipeline?
```

```
Refresh my company data from HubSpot.
```

## Available Tools

The server offers tools for HubSpot management and data retrieval:

| Tool | Purpose |
|------|---------|
| `hubspot_create_contact` | Create contacts with duplicate prevention |
| `hubspot_create_company` | Create companies with duplicate prevention |
| `hubspot_get_company_activity` | Retrieve activity for specific companies |
| `hubspot_get_active_companies` | Retrieve most recently active companies from local storage |
| `hubspot_get_active_contacts` | Retrieve most recently active contacts from local storage |
| `hubspot_get_recent_conversations` | Retrieve recent conversation threads from local storage |
| `hubspot_search_data` | Semantic search across previously retrieved HubSpot data |
| `hubspot_refresh_data` | Refresh data from HubSpot API and store in local databases |

## Performance Features

- **Dual Database Architecture**: 
  - LevelDB for efficient key-value storage using `{object_type}_{object_id}` as keys
  - FAISS for semantic vector search capabilities
- **Timestamped Data**: All tools display when data was last refreshed
- **Manual Refresh Control**: Refresh data on-demand to minimize API usage
- **Thread-Level Indexing**: Stores each conversation thread individually for precise retrieval
- **Embedding Caching**: Uses SentenceTransformer with automatic caching
- **Persistent Storage**: Data persists between sessions in configurable storage directory
- **Multi-platform Support**: Optimized Docker images for various architectures

## Storage Architecture

The server implements a dual-database architecture for optimal performance:

1. **LevelDB**: Fast key-value storage using `{object_type}_{object_id}` as keys
   - Provides direct record lookup by ID
   - Supports efficient prefix queries for all objects of a type
   - Stores timestamps for data freshness tracking

2. **FAISS**: Vector database for semantic search
   - Enables natural language querying across all stored data
   - Indexes data by encoding it into dense vector representations
   - Stores metadata alongside vectors for contextual retrieval

This architecture allows the server to:
- Reduce API calls to HubSpot by using cached data
- Provide fast direct lookups for known records
- Support semantic search for exploratory queries
- Track data freshness with timestamps in tool descriptions

## Setup

### Prerequisites

You'll need a HubSpot access token with these scopes:
- crm.objects.contacts (read/write)
- crm.objects.companies (read/write)
- sales-email-read

### Quick Start

```bash
# Install via Smithery (recommended)
npx -y @smithery/cli@latest install mcp-hubspot --client claude

# Or pull Docker image directly
docker run -e HUBSPOT_ACCESS_TOKEN=your_token buryhuang/mcp-hubspot:latest
```

### Docker Configuration

For manual configuration in Claude desktop:

```json
{
  "mcpServers": {
    "hubspot": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-e", "HUBSPOT_ACCESS_TOKEN=your_token",
        "-v", "/path/to/storage:/storage",  # Optional persistent storage
        "buryhuang/mcp-hubspot:latest"
      ]
    }
  }
}
```

### Building Docker Image

To build the Docker image locally:

```bash
git clone https://github.com/buryhuang/mcp-hubspot.git
cd mcp-hubspot
docker build -t mcp-hubspot .
```

For multi-platform builds:

```bash
docker buildx create --use
docker buildx build --platform linux/amd64,linux/arm64 -t buryhuang/mcp-hubspot:latest --push .
```

## Development

```bash
pip install -e .
```

## License

MIT License 
