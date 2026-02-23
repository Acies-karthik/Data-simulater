import os
from tempfile import NamedTemporaryFile

import pandas as pd


class Exporter:
    def __init__(self, output_dir="output_data"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def to_csv(self, df: pd.DataFrame, table_name: str, index=False):
        """Export dataset to CSV format."""
        file_path = os.path.join(self.output_dir, f"{table_name}.csv")
        df.to_csv(file_path, index=index)
        print(f"Exported {file_path}")
        return file_path

    def to_json(self, df: pd.DataFrame, table_name: str, orient="records"):
        """Export dataset to JSON format."""
        file_path = os.path.join(self.output_dir, f"{table_name}.json")
        df.to_json(file_path, orient=orient, indent=2)
        print(f"Exported {file_path}")
        return file_path
        
    def to_parquet(self, df: pd.DataFrame, table_name: str):
        """Export dataset to Parquet format."""
        file_path = os.path.join(self.output_dir, f"{table_name}.parquet")
        df.to_parquet(file_path, index=False)
        print(f"Exported {file_path}")
        return file_path
        
    def to_sql_inserts(self, df: pd.DataFrame, table_name: str, dialect: str = "postgres"):
        """
        Export dataset to a file containing SQL INSERT statements.
        Handles basic escaping and formatting for SQL.
        """
        file_path = os.path.join(self.output_dir, f"{table_name}_inserts_{dialect}.sql")
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(f"-- Auto-generated INSERT statements for table: {table_name}\n")
            
            # Get columns safely
            columns_str = ", ".join([f'"{c}"' for c in df.columns])
            base_insert = f"INSERT INTO {table_name} ({columns_str}) VALUES\n"
            f.write(base_insert)
            
            # Format row values
            row_count = len(df)
            for idx, row in enumerate(df.itertuples(index=False)):
                values = []
                for val in row:
                    if pd.isna(val):
                        values.append("NULL")
                    elif isinstance(val, (int, float, bool)):
                        values.append(str(val))
                    else:
                        # Escape single quotes in strings for SQL
                        escaped_val = str(val).replace("'", "''")
                        values.append(f"'{escaped_val}'")
                        
                row_str = "    (" + ", ".join(values) + ")"
                
                # Add comma or semicolon based on row position
                if idx < row_count - 1:
                    f.write(row_str + ",\n")
                else:
                    f.write(row_str + ";\n")
                    
        print(f"Exported {file_path} ({row_count} rows)")
        return file_path
