import json
import os
import argparse

def inspect_file(filepath: str, lines_to_read: int = 2):
    if not os.path.exists(filepath):
        print(f"Error: Could not find '{filepath}'.")
        print("Please ensure you have generated a sample first (e.g., 'python synth_telemetry_gen.py --mode sample --size 10').")
        return

    print(f"\n--- INSEPCTING FILE: {filepath} ---")
    print(f"--- ATTEMPTING TO READ: First {lines_to_read} records ---\n")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for i in range(lines_to_read):
            try:
                line = next(f)
                record = json.loads(line)
                
                # Prettify the output
                print(f"=== RECORD {i + 1} ===")
                # Optionally pretty-print the root dict
                formatted_json = json.dumps(record, indent=4)
                
                # To prevent wall of text, let's limit the output size if it's too big, or just print it.
                print(formatted_json)
                print("====================\n")
            except StopIteration:
                print("End of file reached.")
                break
            except Exception as e:
                print(f"Error parsing line {i + 1}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect generated sample jsonl files safely.")
    parser.add_argument("--file", type=str, default="data_generated/samples/sample_50MB.jsonl", help="Path to the JSONL file to inspect.")
    parser.add_argument("--lines", type=int, default=1, help="Number of lines/records to print.")
    
    args = parser.parse_args()
    inspect_file(args.file, args.lines)
