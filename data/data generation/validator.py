import json
import re

def validate_file(filepath="synthetic_sample.jsonl"):
    total_records = 0
    total_fields = 0
    null_fields = 0
    
    uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.I)
    uuid_tests = {"total": 0, "valid": 0}
    
    json_string_tests = {"total": 0, "valid": 0}
    
    schemas_seen = set()

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            total_records += 1
            record = json.loads(line)
            
            # Identify Schema by its keys hash
            keys_tuple = tuple(record.keys())
            schemas_seen.add(hash(keys_tuple))
            
            # Recursive check
            def inspect(obj):
                nonlocal total_fields, null_fields
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        total_fields += 1
                        if v is None or v == "null":
                            null_fields += 1
                        
                        # UUID check based on common patterns
                        if isinstance(v, str) and (len(v) == 36 and v.count('-') == 4):
                            uuid_tests["total"] += 1
                            if uuid_pattern.match(v):
                                uuid_tests["valid"] += 1
                        
                        # JSON-in-string check
                        if isinstance(v, str) and v.startswith('{') and v.endswith('}'):
                            json_string_tests["total"] += 1
                            try:
                                json.loads(v)
                                json_string_tests["valid"] += 1
                            except:
                                pass
                                
                        inspect(v)
                elif isinstance(obj, list):
                    for item in obj:
                        inspect(item)
                        
            inspect(record)

    null_sparsity = (null_fields / total_fields) * 100 if total_fields > 0 else 0
    
    report = {
        "Total Records Analyzed": total_records,
        "Null Sparsity": f"{null_sparsity:.2f}%",
        "UUID Validity": f"{uuid_tests['valid']}/{uuid_tests['total']}",
        "JSON-in-string Validity": f"{json_string_tests['valid']}/{json_string_tests['total']}",
        "Distinct Schemas Count": len(schemas_seen)
    }
    
    with open("validation_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4)
        
if __name__ == "__main__":
    validate_file()
