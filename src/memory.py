import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional

class Memory:
    """
    State management system for the Data Simulator.
    Keeps track of last generated IDs, timestamps, and data pools 
    to ensure incremental loads are relationally accurate.
    """
    
    def __init__(self, catalog_path: str = None):
        if catalog_path is None:
            # Databricks (Linux) uses read-only Repos, so we must save memory state to /tmp
            if os.name == 'nt':
                self.catalog_path = "state/preventual_catalog.json"
            else:
                self.catalog_path = "/tmp/preventual_catalog.json"
        else:
            self.catalog_path = catalog_path
        self.state = {
            "last_run": None,
            "tables": {},
            "pools": {}
        }
        self.load()

    def load(self):
        """Load state from catalog file if it exists."""
        if os.path.exists(self.catalog_path):
            try:
                with open(self.catalog_path, "r") as f:
                    self.state = json.load(f)
                print(f"Loaded memory state from {self.catalog_path}")
            except json.JSONDecodeError:
                print(f"Warning: Could not parse {self.catalog_path}. Starting fresh.")
        else:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.catalog_path), exist_ok=True)
            print("Initialized fresh memory state.")

    def save(self):
        """Persist current state to catalog file."""
        self.state["last_run"] = datetime.now().isoformat()
        
        with open(self.catalog_path, "w") as f:
            json.dump(self.state, f, indent=2)
        print(f"Memory state saved to {self.catalog_path}")

    # --- Table State Tracking (IDs and Timestamps) ---
    
    def get_table_state(self, table_name: str) -> Dict[str, Any]:
        """Get the tracking state for a specific table."""
        if table_name not in self.state["tables"]:
            self.state["tables"][table_name] = {
                "last_id_numeric": 0,
                "last_timestamp": None,
                "row_count": 0
            }
        return self.state["tables"][table_name]

    def update_table_state(self, table_name: str, last_id: int, last_timestamp: str, rows_added: int):
        """Update tracking metrics after generating rows for a table."""
        state = self.get_table_state(table_name)
        state["last_id_numeric"] = last_id
        state["last_timestamp"] = last_timestamp
        state["row_count"] += rows_added

    def get_next_id(self, table_name: str) -> int:
        """Returns the next available numeric ID for a table."""
        state = self.get_table_state(table_name)
        return state["last_id_numeric"] + 1

    # --- Data Pooling (For Foreign Keys and Relational Integrity) ---

    def register_pool(self, pool_name: str, values: List[Any]):
        """
        Register a pool of valid IDs or Values (e.g., user_ids, product_ids).
        This allows future tables to pick from real, already-generated entities.
        """
        self.state["pools"][pool_name] = values

    def get_pool(self, pool_name: str) -> Optional[List[Any]]:
        """Retrieve a specific pool of data."""
        return self.state["pools"].get(pool_name)
        
    def has_pool(self, pool_name: str) -> bool:
        """Check if a pool exists."""
        return pool_name in self.state["pools"] and len(self.state["pools"][pool_name]) > 0

    def clear(self):
        """Wipe memory clean (Useful for initial load reset)."""
        self.state = {
            "last_run": None,
            "tables": {},
            "pools": {}
        }
        self.save()
