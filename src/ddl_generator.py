import json
import os
from pathlib import Path
from src.dialect_mapper import get_sql_type, Dialect

def generate_ddl(schema_path: str, output_dir: str, dialect: Dialect = "postgres") -> None:
    """
    Parses a schema.json file and generates CREATE TABLE SQL statements for the specified dialect.
    """
    with open(schema_path, "r") as f:
        schema = json.load(f)
        
    os.makedirs(output_dir, exist_ok=True)
    
    output_lines = [f"-- Auto-generated DDL for {dialect.upper()} dialect\n"]
    
    for table in schema.get("tables", []):
        table_name = table.get("table_name", "unknown_table")
        columns = table.get("columns", [])
        
        create_stmt = [f"CREATE TABLE {table_name} ("]
        
        col_defs = []
        for col in columns:
            col_name = col.get("name")
            generic_type = col.get("type")
            sql_type = get_sql_type(generic_type, dialect)
            col_defs.append(f"    {col_name} {sql_type}")
            
        create_stmt.append(",\n".join(col_defs))
        create_stmt.append(");\n")
        
        output_lines.extend(create_stmt)
        output_lines.append("\n")
        
    output_path = Path(output_dir) / f"schema_{dialect}.sql"
    with open(output_path, "w") as f:
        f.write("\n".join(output_lines))
    print(f"Generated {dialect.upper()} DDL: {output_path}")

if __name__ == "__main__":
    # Example usage for testing when running directly
    schema_file = "src/schema.json"
    out_dir = "output_data"
    
    generate_ddl(schema_file, out_dir, "postgres")
    generate_ddl(schema_file, out_dir, "snowflake")
    generate_ddl(schema_file, out_dir, "mysql")
    generate_ddl(schema_file, out_dir, "sqlite")
