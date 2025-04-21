import os
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import plyvel

logger = logging.getLogger("mcp_hubspot_leveldb_manager")

class LevelDBManager:
    """Manager for LevelDB storage."""
    
    def __init__(self, storage_dir: str = "/storage"):
        """Initialize the LevelDB manager.
        
        Args:
            storage_dir: Directory to store LevelDB data
        """
        self.storage_dir = storage_dir
        self.db_path = os.path.join(storage_dir, "leveldb")
        self.db = None
        self.last_updated = {}
        
        # Ensure storage directory exists
        self._ensure_storage_dir()
        
        # Initialize database
        self._initialize_db()
    
    def _ensure_storage_dir(self) -> None:
        """Create storage directory if it doesn't exist."""
        if not os.path.exists(self.storage_dir):
            logger.info(f"Creating storage directory: {self.storage_dir}")
            os.makedirs(self.storage_dir, exist_ok=True)
    
    def _initialize_db(self) -> None:
        """Initialize the LevelDB database."""
        try:
            self.db = plyvel.DB(self.db_path, create_if_missing=True)
            logger.info(f"LevelDB initialized at {self.db_path}")
            
            # Load last updated timestamps
            self._load_last_updated()
        except Exception as e:
            logger.error(f"Failed to initialize LevelDB: {str(e)}")
            raise
    
    def _load_last_updated(self) -> None:
        """Load last updated timestamps from DB."""
        try:
            last_updated_key = b'_last_updated'
            value = self.db.get(last_updated_key)
            if value:
                self.last_updated = json.loads(value.decode('utf-8'))
                logger.debug(f"Loaded last updated timestamps: {self.last_updated}")
            else:
                self.last_updated = {}
        except Exception as e:
            logger.error(f"Failed to load last updated timestamps: {str(e)}")
            self.last_updated = {}
    
    def _save_last_updated(self) -> None:
        """Save last updated timestamps to DB."""
        try:
            last_updated_key = b'_last_updated'
            self.db.put(last_updated_key, json.dumps(self.last_updated).encode('utf-8'))
            logger.debug(f"Saved last updated timestamps: {self.last_updated}")
        except Exception as e:
            logger.error(f"Failed to save last updated timestamps: {str(e)}")
    
    def close(self) -> None:
        """Close the database."""
        if self.db:
            self.db.close()
            logger.info("LevelDB connection closed")
    
    def get_last_updated(self, object_type: str) -> Optional[str]:
        """Get the last updated timestamp for a specific object type.
        
        Args:
            object_type: Type of object
            
        Returns:
            Timestamp string or None if not available
        """
        return self.last_updated.get(object_type)
    
    def set_last_updated(self, object_type: str) -> None:
        """Set the last updated timestamp for a specific object type to current time.
        
        Args:
            object_type: Type of object
        """
        self.last_updated[object_type] = datetime.now().isoformat()
        self._save_last_updated()
    
    def put(self, object_type: str, object_id: str, data: Dict[str, Any]) -> None:
        """Store data in LevelDB.
        
        Args:
            object_type: Type of object
            object_id: ID of the object
            data: Object data
        """
        try:
            # Create key
            key = f"{object_type}_{object_id}".encode('utf-8')
            
            # Add timestamp to data
            data['_timestamp'] = datetime.now().isoformat()
            
            # Store data
            self.db.put(key, json.dumps(data).encode('utf-8'))
            logger.debug(f"Stored {object_type} with ID {object_id} in LevelDB")
            
            # Update last updated timestamp
            self.set_last_updated(object_type)
        except Exception as e:
            logger.error(f"Failed to store {object_type} with ID {object_id} in LevelDB: {str(e)}")
    
    def put_bulk(self, object_type: str, items: List[Dict[str, Any]], id_field: str = "id") -> None:
        """Store multiple items in LevelDB.
        
        Args:
            object_type: Type of objects
            items: List of objects to store
            id_field: Field name containing the object ID
        """
        if not items:
            logger.debug(f"No {object_type} items to store")
            return
            
        try:
            batch = self.db.write_batch()
            timestamp = datetime.now().isoformat()
            
            for item in items:
                if id_field not in item:
                    logger.warning(f"Skipping {object_type} item without {id_field}")
                    continue
                    
                object_id = str(item[id_field])
                key = f"{object_type}_{object_id}".encode('utf-8')
                
                # Add timestamp to item
                item['_timestamp'] = timestamp
                
                # Add to batch
                batch.put(key, json.dumps(item).encode('utf-8'))
            
            # Commit batch
            batch.write()
            logger.info(f"Stored {len(items)} {object_type} items in LevelDB")
            
            # Update last updated timestamp
            self.set_last_updated(object_type)
        except Exception as e:
            logger.error(f"Failed to store {object_type} items in LevelDB: {str(e)}")
    
    def get(self, object_type: str, object_id: str) -> Optional[Dict[str, Any]]:
        """Get data from LevelDB.
        
        Args:
            object_type: Type of object
            object_id: ID of the object
            
        Returns:
            Object data or None if not found
        """
        try:
            key = f"{object_type}_{object_id}".encode('utf-8')
            value = self.db.get(key)
            
            if value:
                return json.loads(value.decode('utf-8'))
            else:
                logger.debug(f"No {object_type} with ID {object_id} found in LevelDB")
                return None
        except Exception as e:
            logger.error(f"Failed to get {object_type} with ID {object_id} from LevelDB: {str(e)}")
            return None
    
    def get_all_by_type(self, object_type: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all objects of a specific type.
        
        Args:
            object_type: Type of objects to retrieve
            limit: Maximum number of objects to return
            
        Returns:
            List of objects
        """
        try:
            prefix = f"{object_type}_".encode('utf-8')
            results = []
            
            for key, value in self.db.iterator(prefix=prefix):
                if limit is not None and len(results) >= limit:
                    break
                    
                try:
                    item = json.loads(value.decode('utf-8'))
                    results.append(item)
                except Exception as e:
                    logger.error(f"Failed to parse item: {str(e)}")
            
            # Sort by timestamp, newest first
            results.sort(key=lambda x: x.get('_timestamp', ''), reverse=True)
            
            logger.debug(f"Retrieved {len(results)} {object_type} items from LevelDB")
            return results
        except Exception as e:
            logger.error(f"Failed to get {object_type} items from LevelDB: {str(e)}")
            return []
    
    def delete(self, object_type: str, object_id: str) -> bool:
        """Delete data from LevelDB.
        
        Args:
            object_type: Type of object
            object_id: ID of the object
            
        Returns:
            True if successful, False otherwise
        """
        try:
            key = f"{object_type}_{object_id}".encode('utf-8')
            self.db.delete(key)
            logger.debug(f"Deleted {object_type} with ID {object_id} from LevelDB")
            return True
        except Exception as e:
            logger.error(f"Failed to delete {object_type} with ID {object_id} from LevelDB: {str(e)}")
            return False 