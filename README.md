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

## Example Prompts

```
Create HubSpot contacts and companies from this LinkedIn profile:
[Paste LinkedIn profile text]
```

```
What's happening lately with my pipeline?
```

## Available Tools

The server offers tools for HubSpot management and data retrieval:

| Tool | Purpose |
|------|---------|
| `hubspot_create_contact` | Create contacts with duplicate prevention |
| `hubspot_create_company` | Create companies with duplicate prevention |
| `hubspot_get_company_activity` | Retrieve activity for specific companies |
| `hubspot_get_active_companies` | Retrieve most recently active companies |
| `hubspot_get_active_contacts` | Retrieve most recently active contacts |
| `hubspot_get_recent_conversations` | Retrieve recent conversation threads with messages |
| `hubspot_search_data` | Semantic search across previously retrieved HubSpot data |

## Performance Features

- **Vector Storage**: Utilizes FAISS for efficient semantic search and retrieval
- **Thread-Level Indexing**: Stores each conversation thread individually for precise retrieval
- **Embedding Caching**: Uses SentenceTransformer with automatic caching
- **Persistent Storage**: Data persists between sessions in configurable storage directory
- **Multi-platform Support**: Optimized Docker images for various architectures

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

## Development

```bash
pip install -e .
```

## License

MIT License 
