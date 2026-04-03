import json
import random
import uuid
import hashlib
import datetime
import os
import copy
import argparse
import logging
import re
from typing import Dict, Any, List

SEED = 42

# --- STRUCTURE SETUP ---
DATA_DIR = "data_generated"
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw_data")
SAMPLES_DIR = os.path.join(DATA_DIR, "samples")
SCHEMAS_DIR = os.path.join(DATA_DIR, "schemas")
REPORTS_DIR = os.path.join(DATA_DIR, "reports")
LOGS_DIR = os.path.join(DATA_DIR, "logs")

for d in [DATA_DIR, RAW_DATA_DIR, SAMPLES_DIR, SCHEMAS_DIR, REPORTS_DIR, LOGS_DIR]:
    os.makedirs(d, exist_ok=True)

# --- LOGGING SETUP ---
log_file = os.path.join(LOGS_DIR, "generation_log.txt")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- CORE ENGINE ---

class RandomProvider:
    def __init__(self, seed: int):
        self.rng = random.Random(seed)

    def uuid4(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def hash256(self) -> str:
        return hashlib.sha256(self.rng.randbytes(64)).hexdigest()
        
    def rand_int(self, min_val: int, max_val: int) -> int:
        return self.rng.randint(min_val, max_val)

    def rand_choice(self, choices: list) -> Any:
        return self.rng.choice(choices)
        
    def rand_float(self, min_val: float, max_val: float) -> float:
        return self.rng.uniform(min_val, max_val)

    def probability(self, p: float) -> bool:
        return self.rng.random() < p

def is_timestamp(val: str) -> bool:
    try:
        if len(val) >= 10 and (val[4] == '-' or val[4] == '/'):
            return True
    except:
        pass
    return False

def is_uuid(val: str) -> bool:
    try:
        uuid.UUID(val)
        return True
    except:
        return False

def is_hex_hash(val: str) -> bool:
    return len(val) >= 32 and all(c in '0123456789abcdefABCDEF' for c in val)

class SchemaParser:
    @staticmethod
    def parse_value(val: Any) -> Dict[str, Any]:
        if val is None:
            return {"type": "null"}
        elif isinstance(val, bool):
            return {"type": "boolean"}
        elif isinstance(val, int):
            return {"type": "integer"}
        elif isinstance(val, float):
            return {"type": "float"}
        elif isinstance(val, list):
            if len(val) > 0:
                return {"type": "array", "item_type": SchemaParser.parse_value(val[0])}
            return {"type": "array", "item_type": {"type": "string"}}
        elif isinstance(val, dict):
            props = {}
            for k, v in val.items():
                props[k] = SchemaParser.parse_value(v)
            return {"type": "object", "properties": props}
        elif isinstance(val, str):
            try:
                parsed_json = json.loads(val)
                if isinstance(parsed_json, (dict, list)):
                    return {"type": "json_string", "structure": SchemaParser.parse_value(parsed_json)}
            except:
                pass
            
            if is_uuid(val):
                return {"type": "string_uuid"}
            elif is_timestamp(val):
                return {"type": "string_timestamp", "format": "iso"}
            elif is_hex_hash(val):
                return {"type": "string_hash"}
            elif val.lower() in ("true", "false"):
                return {"type": "string_boolean"}
            elif val.isdigit():
                return {"type": "string_integer"}
            else:
                return {"type": "string_enum", "sample": val}
        return {"type": "string"}

    @staticmethod
    def extract_from_file(filepath: str) -> Dict[str, Any]:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        record = data[0] if isinstance(data, list) and len(data) > 0 else data
        return SchemaParser.parse_value(record)

def cluster_properties(properties: Dict[str, Any]) -> Dict[str, Any]:
    sorted_keys = sorted(properties.keys(), key=lambda x: x.split('_')[0] if '_' in x else x)
    return {k: properties[k] for k in sorted_keys}

def build_schema_templates(input_files: List[str], target_count: int, rng: RandomProvider) -> List[Dict[str, Any]]:
    logger.info("Parsing inputs and extracting base AST templates...")
    base_templates = []
    for fp in input_files:
        parsed = SchemaParser.extract_from_file(fp)
        if parsed.get("type") == "object":
            parsed["properties"] = cluster_properties(parsed["properties"])
        base_templates.append(parsed)
    
    schemas = []
    for i in range(target_count):
        base = copy.deepcopy(rng.rand_choice(base_templates))
        if base.get("type") == "object":
            props = base["properties"]
            new_props = {}
            for k, v in props.items():
                if rng.probability(0.85): 
                    new_props[k] = v
                if rng.probability(0.1) and '_' in k:
                    parts = k.split('_', 1)
                    new_k = f"{parts[0]}{rng.rand_int(1, 5)}_{parts[1]}" if len(parts)>1 else f"{k}_{rng.rand_int(1, 5)}"
                    new_props[new_k] = copy.deepcopy(v)
            base["properties"] = cluster_properties(new_props)
        schemas.append({"schema_id": i, "template": base})
    
    # EXPORT SCHEMAS
    for s in schemas:
        s_path = os.path.join(SCHEMAS_DIR, f"schema_{s['schema_id']}.json")
        with open(s_path, 'w') as f:
            # Explicitly sort keys for AST predictability
            json.dump(s, f, indent=2, sort_keys=True)
            
    logger.info(f"Successfully generated and exported {len(schemas)} schemas to {SCHEMAS_DIR}/")
    return schemas

class DeviceState:
    def __init__(self, tenant_ip: str, machine_model: str, manufacturer: str, rp: RandomProvider):
        self.device_uuid = rp.uuid4()
        self.tenant_ip = tenant_ip
        self.machine_model = machine_model
        self.manufacturer = manufacturer
        self.current_time = datetime.datetime(2026, 1, 1, 12, 0, 0)
    
    def next_tick(self, rp: RandomProvider) -> datetime.datetime:
        self.current_time += datetime.timedelta(seconds=rp.rand_int(1, 600))
        return self.current_time

class DataGenerator:
    def __init__(self, rp: RandomProvider):
        self.rp = rp
        self.device_pool = [
            DeviceState(f"192.168.1.{i}", f"Model-X{i%10}", f"Maker-{i%3}", rp)
            for i in range(100)
        ]
        self.null_prob = 0.5

    def generate_value(self, schema: Dict[str, Any], device: DeviceState, event_time: datetime.datetime, tied_ids: dict) -> Any:
        t = schema.get("type")
        if t not in ("object", "array", "json_string") and self.rp.probability(self.null_prob):
            return None if self.rp.probability(0.5) else "null"

        if t == "null":
            return None
        elif t == "boolean":
            return self.rp.probability(0.5)
        elif t == "integer":
            return self.rp.rand_int(0, 10000)
        elif t == "float":
            return round(self.rp.rand_float(0.0, 100.0), 4)
        elif t == "string_boolean":
            return "true" if self.rp.probability(0.5) else "false"
        elif t == "string_integer":
            return str(self.rp.rand_int(0, 1000000))
        elif t == "string_uuid":
            if self.rp.probability(0.3):
                return device.device_uuid
            if self.rp.probability(0.2) and "last_uuid" in tied_ids:
                return tied_ids["last_uuid"]
            new_id = self.rp.uuid4()
            tied_ids["last_uuid"] = new_id
            return new_id
        elif t == "string_timestamp":
            return event_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
        elif t == "string_hash":
            return self.rp.hash256()
        elif t == "string_enum":
            sample = schema.get("sample", "unknown")
            prefixes = ["valid", "error", "pending", "success", "invalidBrewFormat", "MaxMind"]
            return self.rp.rand_choice(prefixes + [sample])
        elif t == "string":
            return f"val_{self.rp.rand_int(1, 1000)}"
        elif t == "array":
            length = self.rp.rand_int(0, 5)
            return [self.generate_value(schema["item_type"], device, event_time, tied_ids) for _ in range(length)]
        elif t == "object":
            obj = {}
            for k, v in schema["properties"].items():
                obj[k] = self.generate_value(v, device, event_time, tied_ids)
            return obj
        elif t == "json_string":
            inner_obj = self.generate_value(schema["structure"], device, event_time, tied_ids)
            # DOUBLE SERIALIZATION FIX: Ensure inner nested structure is an elegant dumped string
            # Also guarantees identical key-sorting internally
            return json.dumps(inner_obj, separators=(',', ':'), sort_keys=True)
            
        return None

    def generate_record(self, schema_def: Dict[str, Any]) -> dict:
        device = self.rp.rand_choice(self.device_pool)
        event_time = device.next_tick(self.rp)
        return self.generate_value(schema_def["template"], device, event_time, {})

# --- VALIDATOR ---
def validate_output(files_to_check: List[str]):
    logger.info("Initializing Validation Phase...")
    total_records = 0
    total_fields = 0
    null_fields = 0
    
    uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.I)
    uuid_tests = {"total": 0, "valid": 0}
    json_string_tests = {"total": 0, "valid": 0}
    schemas_seen = set()

    for fp in files_to_check:
        with open(fp, 'r', encoding='utf-8') as f:
            for line in f:
                total_records += 1
                record = json.loads(line)
                schemas_seen.add(hash(tuple(record.keys())))
                
                def inspect(obj):
                    nonlocal total_fields, null_fields
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            total_fields += 1
                            if v is None or v == "null":
                                null_fields += 1
                            
                            if isinstance(v, str) and len(v) == 36 and v.count('-') == 4:
                                uuid_tests["total"] += 1
                                if uuid_pattern.match(v):
                                    uuid_tests["valid"] += 1
                            
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
    
    with open(os.path.join(REPORTS_DIR, "validation_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4)
    
    logger.info("Validation completed. Results saved to validation_report.json.")
    logger.info(json.dumps(report, indent=4))

# --- PIPELINE ---

def run_pipeline(mode: str, total_mb: int):
    logger.info(f"Starting pipeline in [{mode}] mode. Target size: {total_mb}MB.")
    rp = RandomProvider(SEED)
    inputs = [
        "input_schemas/Synthetic_IoT_data_1.json",
        "input_schemas/Synthetic_IoT_data_2.json",
        "input_schemas/Synthetic_IoT_data_3.json"
    ]
    schemas = build_schema_templates(inputs, 50, rp)
    
    schema_weights = []
    for s in schemas:
        if s["schema_id"] < 10:
            schema_weights.append(0.60 / 10)
        elif s["schema_id"] < 30:
            schema_weights.append(0.30 / 20)
        else:
            schema_weights.append(0.10 / 20)
            
    generator = DataGenerator(rp)
    
    if mode == "sample":
        output_f = os.path.join(SAMPLES_DIR, f"sample_{total_mb}MB.jsonl")
        files_to_write = [(output_f, total_mb * 1024 * 1024)]
    else:
        # Full mode: twenty 1GB files
        CHUNKS = 20
        # Determine exact size per chunk
        chunk_bytes = (total_mb * 1024 * 1024) // CHUNKS
        files_to_write = []
        for i in range(CHUNKS):
            files_to_write.append((os.path.join(RAW_DATA_DIR, f"part_{i:02d}.jsonl"), chunk_bytes))

    written_files = []
    
    for f_target, t_bytes in files_to_write:
        logger.info(f"Writing to target file: {f_target} (Target Size: {t_bytes / (1024*1024):.2f}MB)")
        written_bytes = 0
        records = 0
        with open(f_target, 'w', encoding='utf-8') as f:
            # WARMUP PHASE: Ensure all 50 schemas explicitly exist upfront
            # Doing this on every file opening just to bulletproof it
            for schema in schemas:
                record = generator.generate_record(schema)
                # DETERMINISTIC KEY ORDERING FIX: set sort_keys=True on the ROOT payload dump too
                line = json.dumps(record, sort_keys=True) + '\n'
                f.write(line)
                written_bytes += len(line.encode('utf-8'))
                records += 1

            # INTERLEAVED LOOP phase
            while written_bytes < t_bytes:
                s = rp.rng.choices(schemas, weights=schema_weights, k=1)[0]
                record = generator.generate_record(s)
                line = json.dumps(record, sort_keys=True) + '\n'
                f.write(line)
                written_bytes += len(line.encode('utf-8'))
                records += 1
                
                if records % 5000 == 0:
                    logger.info(f"Progress [{f_target}]: {written_bytes / (1024 * 1024):.2f} MB written.")

        logger.info(f"Done building file {f_target}. Total records: {records}.")
        written_files.append(f_target)
        
    validate_output(written_files)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HP Synthetic Telemetry Generator")
    parser.add_argument("--mode", type=str, choices=["sample", "full"], required=True, help="Mode: 'sample' or 'full'.")
    parser.add_argument("--size", type=int, required=True, help="Total target size in MB (e.g., 50 for sample, 20480 for full).")
    args = parser.parse_args()
    
    run_pipeline(args.mode, args.size)
