"""Utility functions for HubSpot MCP server."""

import json
import logging
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from sentence_transformers import SentenceTransformer
from .faiss_manager import FaissManager
from .leveldb_manager import LevelDBManager

logger = logging.getLogger("mcp_hubspot_utils")

def generate_embeddings(data: List[Dict[str, Any]], model: SentenceTransformer) -> np.ndarray:
    """Generate embeddings for a list of data items.
    
    Args:
        data: List of data items to generate embeddings for
        model: SentenceTransformer model to use
        
    Returns:
        NumPy array of embeddings
    """
    texts = [json.dumps(item) for item in data]
    return np.array(model.encode(texts))

def store_in_leveldb(
    leveldb_manager: LevelDBManager, 
    data: List[Dict[str, Any]], 
    data_type: str
) -> None:
    """Store data in LevelDB.
    
    Args:
        leveldb_manager: LevelDB manager instance
        data: List of data items to store
        data_type: Type of data (company, contact, engagement, etc.)
    """
    try:
        logger.debug(f"Starting store_in_leveldb for {data_type} with {len(data) if data else 0} items")
        
        if not data:
            logger.info(f"No {data_type} data to store in LevelDB")
            return
            
        # Store in LevelDB
        leveldb_manager.put_bulk(data_type, data)
        logger.info(f"Successfully stored {len(data)} {data_type} items in LevelDB")
    except Exception as e:
        logger.error(f"Error storing {data_type} in LevelDB: {str(e)}", exc_info=True)

def store_in_faiss(
    faiss_manager: FaissManager, 
    data: List[Dict[str, Any]], 
    data_type: str,
    model: SentenceTransformer,
    metadata_extras: Optional[Dict[str, Any]] = None
) -> None:
    """Store data in FAISS index.
    
    Args:
        faiss_manager: FAISS manager instance
        data: List of data items to store
        data_type: Type of data (company, contact, engagement, etc.)
        model: SentenceTransformer model to use
        metadata_extras: Additional metadata to store with each item
    """
    try:
        logger.debug(f"Starting store_in_faiss for {data_type} with {len(data) if data else 0} items")
        logger.debug(f"Metadata extras: {metadata_extras}")
        
        if not data:
            logger.info(f"No {data_type} data to store in FAISS")
            return
            
        # Generate embeddings
        logger.debug(f"Generating embeddings for {len(data)} {data_type} items")
        embeddings = generate_embeddings(data, model)
        logger.debug(f"Generated embeddings with shape: {embeddings.shape}")
        
        # Create metadata list
        logger.debug(f"Creating metadata for {len(data)} {data_type} items")
        metadata_list = []
        for item in data:
            metadata = {
                "type": data_type,
                "data": item
            }
            if metadata_extras:
                metadata.update(metadata_extras)
            metadata_list.append(metadata)
        
        # Store in FAISS
        logger.debug(f"Adding {len(embeddings)} vectors to FAISS index")
        faiss_manager.add_data(vectors=embeddings, metadata_list=metadata_list)
        logger.info(f"Successfully stored {len(data)} {data_type} items in FAISS")
    except Exception as e:
        logger.error(f"Error storing {data_type} in FAISS: {str(e)}", exc_info=True)

def get_from_leveldb(
    leveldb_manager: LevelDBManager,
    data_type: str,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """Retrieve data from LevelDB.
    
    Args:
        leveldb_manager: LevelDB manager instance
        data_type: Type of data to retrieve
        limit: Maximum number of items to retrieve
        
    Returns:
        List of data items
    """
    try:
        logger.debug(f"Retrieving {data_type} data from LevelDB with limit {limit}")
        items = leveldb_manager.get_all_by_type(data_type, limit=limit)
        logger.info(f"Retrieved {len(items)} {data_type} items from LevelDB")
        return items
    except Exception as e:
        logger.error(f"Error retrieving {data_type} from LevelDB: {str(e)}", exc_info=True)
        return []

def search_in_faiss(
    faiss_manager: FaissManager, 
    query: str,
    model: SentenceTransformer,
    limit: int = 10
) -> Tuple[List[Dict[str, Any]], List[float]]:
    """Search in FAISS index.
    
    Args:
        faiss_manager: FAISS manager instance
        query: Text query to search for
        model: SentenceTransformer model to use
        limit: Maximum number of results to return
        
    Returns:
        Tuple of (formatted_results, distances)
    """
    try:
        # Generate embedding for the query
        query_embedding = model.encode(query)
        
        # Search in FAISS
        metadata_list, distances = faiss_manager.search(
            query_vector=np.array(query_embedding),
            k=limit
        )
        
        # Format results
        results = []
        for i, (metadata, distance) in enumerate(zip(metadata_list, distances)):
            results.append({
                "rank": i + 1,
                "similarity_score": 1.0 - (distance / 2.0),  # Convert distance to similarity score (0-1)
                "type": metadata.get("type", "unknown"),
                "data": metadata.get("data", {})
            })
        
        return results, distances
    except Exception as e:
        logger.error(f"Error searching in FAISS: {str(e)}")
        raise

def refresh_data(
    hubspot_client,
    data_type: str,
    faiss_manager: FaissManager,
    leveldb_manager: LevelDBManager,
    model: SentenceTransformer,
    limit: int = 100,
    after: Optional[str] = None,
    store_all_pages: bool = False
) -> Dict[str, Any]:
    """Refresh data from HubSpot API and store in LevelDB and FAISS.
    
    Args:
        hubspot_client: HubSpot client instance
        data_type: Type of data to refresh
        faiss_manager: FAISS manager instance
        leveldb_manager: LevelDB manager instance
        model: SentenceTransformer model to use
        limit: Maximum number of items to fetch per page
        after: Pagination token for fetching next page
        store_all_pages: Whether to fetch and store all available pages (caution: may exceed API limits)
        
    Returns:
        Dictionary with refresh status, timestamp, and pagination token
    """
    try:
        logger.info(f"Refreshing {data_type} data with limit={limit}, after={after}")
        
        # Initialize result object
        result = {
            "status": "success",
            "data_type": data_type,
            "count": 0,
            "pagination": {"next": {"after": None}}
        }
        
        # Fetch data from HubSpot API based on data_type
        data = None
        next_after = None
        response = None
        
        if data_type == "company":
            # Get companies with pagination
            response = hubspot_client.get_recent_companies(limit=limit, after=after)
            data = response.get("results", [])
            next_after = response.get("pagination", {}).get("next", {}).get("after")
            result["pagination"]["next"]["after"] = next_after
            
        elif data_type == "contact":
            # Get contacts with pagination
            response = hubspot_client.get_recent_contacts(limit=limit, after=after)
            data = response.get("results", [])
            next_after = response.get("pagination", {}).get("next", {}).get("after")
            result["pagination"]["next"]["after"] = next_after
            
        elif data_type == "conversation_thread":
            # Conversations have pagination support
            response = hubspot_client.get_recent_conversations(
                limit=limit, 
                after=after, 
                refresh_cache=True
            )
            data = response.get("results", [])
            next_after = response.get("pagination", {}).get("next", {}).get("after")
            result["pagination"]["next"]["after"] = next_after
            
        else:
            raise ValueError(f"Unsupported data type: {data_type}")
        
        # If we received an error in the response
        if response and "error" in response:
            logger.error(f"API returned error: {response['error']}")
            return {
                "status": "error",
                "error": response["error"],
                "data_type": data_type,
                "pagination": {"next": {"after": None}}
            }
        
        # If no data was returned, return early
        if not data:
            logger.info(f"No {data_type} data was returned from API")
            result["count"] = 0
            return result
            
        # Store in LevelDB
        store_in_leveldb(leveldb_manager, data, data_type)
        
        # Store in FAISS
        metadata_extras = {"limit": limit, "after": after}
        store_in_faiss(faiss_manager, data, data_type, model, metadata_extras)
        
        # Save FAISS index
        faiss_manager.save_today_index()
        
        # Get updated timestamp
        timestamp = leveldb_manager.get_last_updated(data_type)
        result["timestamp"] = timestamp
        result["count"] = len(data)
        
        # If we should fetch all pages and there's a next page available
        if store_all_pages and next_after:
            logger.info(f"Fetching next page with after={next_after}")
            next_result = refresh_data(
                hubspot_client=hubspot_client,
                data_type=data_type,
                faiss_manager=faiss_manager,
                leveldb_manager=leveldb_manager,
                model=model,
                limit=limit,
                after=next_after,
                store_all_pages=True
            )
            
            # Update count to include all pages
            if next_result["status"] == "success":
                result["count"] += next_result["count"]
                # Take the last page's next token
                result["pagination"]["next"]["after"] = next_result["pagination"]["next"]["after"]
        
        return result
    except Exception as e:
        logger.error(f"Error refreshing {data_type} data: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": str(e),
            "data_type": data_type,
            "pagination": {"next": {"after": None}}
        } 