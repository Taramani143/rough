# Synthetic HP Telemetry Data Generator

## 🚀 Overview

This project provides a **production-grade synthetic telemetry data generator** designed to mimic real-world HP telemetry streams for **lossless compression research**.

It generates a **20GB dataset** with **50 distinct schemas**, preserving key real-world characteristics:

* Deeply nested structures
* JSON-in-string fields (double-serialized)
* High-entropy UUIDs and hashes
* Sparse fields (40–70% nulls)
* Mixed schema ingestion (interleaved streams)
* Fully deterministic output (**SEED = 42**)

---

## ⚡ Quick Start (Run in 30 seconds)

```bash
# Clone repo
git clone <your-repo>
cd <your-repo>

# Generate small sample (recommended)
python synth_telemetry_gen.py --mode sample --size 50
```

👉 Output:

* `data_generated/samples/sample_50MB.jsonl`
* schemas + validation report

---

## 📋 Prerequisites

* Python 3.8+
* Any terminal (VS Code / CMD / Bash)

✅ No external dependencies required

---

## 💻 Usage

### 1. Generate Sample Data

```bash
python synth_telemetry_gen.py --mode sample --size 50
```

* Fast (~seconds)
* Inspectable
* Recommended before full run

---

### 2. Generate Full Dataset (20GB)

```bash
python synth_telemetry_gen.py --mode full --size 20480
```

⏱ Runtime: ~45–120 minutes
💾 Disk required: ~25–30GB free

Output:

```
data_generated/raw_data/
├── part_00.jsonl
├── ...
└── part_19.jsonl
```

---

## 📂 Output Structure

```
data_generated/
├── raw_data/     # 20GB dataset (ignored in Git)
├── samples/      # small inspectable data
├── schemas/      # 50 schema templates
├── reports/      # validation metrics
└── logs/         # execution logs
```

---

## 🔍 Inspecting the Data

⚠️ Do NOT open large `.jsonl` files in editors.

### Option 1: Use helper script

```bash
python inspect_sample.py --file data_generated/samples/sample_50MB.jsonl --lines 1
```

### Option 2: Terminal preview

```bash
head -n 5 data_generated/samples/sample_50MB.jsonl
```

---

## 🧠 What Makes This Dataset Realistic?

This generator mimics real telemetry behavior:

| Feature              | Description                 |
| -------------------- | --------------------------- |
| Multi-schema streams | 50 schemas interleaved      |
| High entropy         | UUIDs, hashes               |
| Redundancy           | repeated prefixes           |
| Sparse fields        | ~54% null                   |
| Nested JSON strings  | real serialization overhead |
| Time-series          | sequential timestamps       |

---

## 🛡 Reproducibility

* Global seed: `SEED = 42`
* Fully deterministic
* Same dataset across all machines

---

## 📊 Validation Metrics

Each run generates:

```
data_generated/reports/validation_report.json
```

Includes:

* Null sparsity (target: 40–70%)
* UUID correctness
* JSON-in-string validity
* Schema count (50)

---

## ⚠️ Notes

* Full dataset (20GB) is NOT included in this repo
* It must be generated locally
* Use sample files for inspection

---

## 🎯 Use Cases

* Compression research (ARCH pipelines)
* Schema-aware optimization
* Big data benchmarking
* Storage efficiency studies

---

## 📌 Summary

This project provides a **realistic, scalable, and reproducible telemetry dataset generator**, suitable for **industry-grade compression experiments**.
