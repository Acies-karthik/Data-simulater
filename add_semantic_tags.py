import json

def assign_semantic_tag(name: str, generic_type: str) -> str:
    name_lower = name.lower()
    
    # 1. Identifiers
    if generic_type == "identifier":
        if "sku" in name_lower or "barcode" in name_lower:
            return "sku_code"
        if "serial" in name_lower:
            return "serial_code"
        if "terminal_id" in name_lower or "pos_" in name_lower or "device" in name_lower:
            return "terminal_code"
        if "session" in name_lower or "review_id" in name_lower or "stream_id" in name_lower:
            return "uuid"
        return "auto_increment_id" # Default for *_id
        
    # 2. Datetimes
    if generic_type == "datetime":
        if "birth" in name_lower or "dob" in name_lower:
            return "date_of_birth"
        return "past_datetime" # generic datetime
        
    # 3. Categorical (Strings that represent a finite set)
    if generic_type == "categorical":
        if "status" in name_lower or "state" in name_lower:
            return "status_category"
        if "country" in name_lower:
            return "country_code"
        if "currency" in name_lower:
            return "currency_code"
        if "gender" in name_lower:
            return "gender"
        if "language" in name_lower:
            return "language_code"
        return "generic_category"
        
    # 4. Numbers
    if generic_type == "number":
        if "price" in name_lower or "amount" in name_lower or "salary" in name_lower or "cost" in name_lower or "fee" in name_lower or "balance" in name_lower:
            return "financial_numeric"
        if "age" in name_lower:
            return "age_numeric"
        if "rating" in name_lower or "score" in name_lower:
            return "rating_numeric"
        if "latitude" in name_lower:
            return "latitude"
        if "longitude" in name_lower:
            return "longitude"
        if "id" in name_lower and "count" not in name_lower:
            return "foreign_key_id"
        return "generic_numeric"
        
    # 5. Strings (Free text, names, emails, addresses)
    if generic_type == "string":
        if "first_name" in name_lower:
            return "first_name"
        if "last_name" in name_lower:
            return "last_name"
        if "name" in name_lower:
            return "person_name"
        if "email" in name_lower:
            return "email"
        if "phone" in name_lower:
            return "phone_number"
        if "address" in name_lower:
            return "street_address"
        if "city" in name_lower:
            return "city"
        if "zip" in name_lower or "postal" in name_lower:
            return "postcode"
        if "url" in name_lower or "link" in name_lower:
            return "url"
        if "ip" in name_lower:
            return "ipv4"
        if "description" in name_lower or "text" in name_lower or "bio" in name_lower or "comments" in name_lower or "notes" in name_lower:
            return "text_paragraph"
        if "code" in name_lower or "number" in name_lower or "mac" in name_lower:
            if "sku" in name_lower or "barcode" in name_lower:
                return "sku_code"
            if "serial" in name_lower:
                return "serial_code"
            return "alphanumeric_code"
        if "job" in name_lower or "title" in name_lower:
            return "job_title"
        if "company" in name_lower or "manufacturer" in name_lower or "brand" in name_lower:
            return "company_name"
            
        return "generic_string"

    return "generic_string"

def update_schema():
    file_path = "src/schema.json"
    
    with open(file_path, "r") as f:
        data = json.load(f)
        
    for table in data.get("tables", []):
        for col in table.get("columns", []):
            # Generate and append a semantic tag based on name heuristics
            col["semantic_tag"] = assign_semantic_tag(col["name"], col["type"])
            
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)
        
    print(f"Successfully updated {file_path} with 'semantic_tag' mapping.")

if __name__ == "__main__":
    update_schema()
