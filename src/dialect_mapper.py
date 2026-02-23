from typing import Dict, Literal

Dialect = Literal["postgres", "mysql", "snowflake", "sqlite"]
GenericType = Literal["identifier", "datetime", "categorical", "number", "string"]

DIALECT_MAPPINGS: Dict[Dialect, Dict[GenericType, str]] = {
    "postgres": {
        "identifier": "UUID",
        "datetime": "TIMESTAMP",
        "categorical": "VARCHAR(100)",
        "number": "NUMERIC(18, 4)",
        "string": "TEXT"
    },
    "mysql": {
        "identifier": "VARCHAR(36)",
        "datetime": "DATETIME",
        "categorical": "VARCHAR(100)",
        "number": "DECIMAL(18, 4)",
        "string": "TEXT"
    },
    "snowflake": {
        "identifier": "VARCHAR(36)",
        "datetime": "TIMESTAMP_NTZ",
        "categorical": "VARCHAR(100)",
        "number": "NUMBER(18, 4)",
        "string": "VARCHAR(16777216)"  # Snowflake max size
    },
    "sqlite": {
        "identifier": "TEXT",
        "datetime": "DATETIME",
        "categorical": "TEXT",
        "number": "REAL",
        "string": "TEXT"
    }
}

def get_sql_type(generic_type: str, dialect: Dialect = "postgres") -> str:
    """
    Translates a generic data type to a database-specific SQL dialect.
    
    Args:
        generic_type: The generic type from the schema ('identifier', 'datetime', 'categorical', 'number', 'string')
        dialect: The target SQL dialect ('postgres', 'mysql', 'snowflake', 'sqlite')
        
    Returns:
        The SQL data type string
    """
    if dialect not in DIALECT_MAPPINGS:
        raise ValueError(f"Unsupported dialect: {dialect}. Supported dialects are: {list(DIALECT_MAPPINGS.keys())}")
        
    if generic_type not in DIALECT_MAPPINGS[dialect]:
        # Fallback to string behavior for unknown types
        return DIALECT_MAPPINGS[dialect]["string"]
        
    return DIALECT_MAPPINGS[dialect][generic_type]
