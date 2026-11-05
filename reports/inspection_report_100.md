# TAR Record Dataset Inspection Report

**Source Dataset:** `data/sample_100.jsonl`  
**Protocol Target:** Time-Aware Reliable Transport (TAR) Phase 1  

## 1. Summary of Integrity
- **Total Records:** 100
- **Sequential & Unique IDs Confirmed:** [YES]
- **ID Range:** `1 to 100`
- **Validation Errors:** 0

## 2. Three-Metric Payload and Datagram Size Breakdown

| Metric | Min Bytes | Max Bytes | Avg Bytes | Total Bytes | Limit / Ceiling | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`value_bytes`** (raw application string) | 0 | 962 | 201.0 | 20,100 | N/A | [OK] Validated |
| **`serialized_logical_payload_bytes`** (compact JSON) | 31 | 996 | 233.38 | 23,338 | 1000 bytes max | [OK] Within limit |
| **`estimated_complete_data_datagram_bytes`** (DATA packet) | 139 | 1106 | 343.22 | 34,322 | 1200 bytes MTU | [OK] Projected safe |

### Accounting for Empty Payloads
> **Why empty values occupy bytes:**  
> Why empty values occupy bytes: While an empty payload has value_bytes = 0 (the string is empty), its serialized_logical_payload_bytes is 31 bytes because the payload is structured JSON containing dictionary keys, quotation marks, and category metadata: {"value":"","category":"empty"}. When encapsulated in a DATA packet envelope, its estimated complete datagram size is approximately 144 bytes.

### Datagram Size Enforcement & Verification
> **MTU Ceiling (1,200 Bytes):**  
> The complete serialized DATA datagram must never exceed 1,200 UTF-8 bytes on the wire to prevent IP fragmentation. 
> In Phase 1, logical payloads are strictly validated against the configurable 1,000-byte default ceiling (`DEFAULT_MAX_PAYLOAD_BYTES`), 
> ensuring ample headroom (~200 bytes) for the DATA packet envelope.  
> *Note: Formal verification of the complete 1,200-byte datagram limit using the live packet serializer will be added in Phase 2/3.*

## 3. Edge-Case Breakdown
- **Unicode Records:** 8 (multi-byte UTF-8, Asian scripts, Arabic, Cyrillic, math symbols, Emojis)
- **Empty Payload Records:** 7 (0-byte payload values)
- **Boundary Records (≥900 bytes):** 8 (calibrated near 1,000-byte payload limit)
- **Large Records (400–800 bytes):** 9 (strictly between 400 and 800 bytes)

### Category Distribution
| Category | Count | Percentage |
| :--- | :--- | :--- |
| `normal` | 65 | 65.0% |
| `empty` | 7 | 7.0% |
| `unicode` | 11 | 11.0% |
| `large` | 9 | 9.0% |
| `boundary` | 8 | 8.0% |
| `other` | 0 | 0.0% |

## 4. First 100 Records Verification Summary
- **Total Records Inspected:** 100
- **All 100 IDs Strictly Sequential:** [YES]
- **Dedicated Evidence File:** `reports/first_100_records_inspection.md`

| ID | Category | Value Bytes | Serialized Bytes | Datagram Bytes | Unicode | Empty | Preview | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | `normal` | 144 | 176 | 284 | No | No | `Record 1: datagram recovery acknowledgment da` | PASS |
| 2 | `empty` | 0 | 31 | 139 | No | Yes | `` | PASS |
| 3 | `unicode` | 97 | 139 | 247 | No | No | `Special delimiters & escaped JSON quotes: {"k` | PASS |
| 4 | `large` | 679 | 710 | 818 | No | No | `Record 4 LARGE_PAYLOAD: ABCDEF0123456789-ABCD` | PASS |
| 5 | `boundary` | 962 | 996 | 1104 | No | No | `Record 5 BOUNDARY_MAX_SIZE: XXXXXXXXXXXXXXXXX` | PASS |
| 6 | `normal` | 106 | 138 | 246 | No | No | `Record 6: acknowledgment session monotonic ne` | PASS |
| 7 | `unicode` | 68 | 101 | 209 | Yes | No | `TAR transport protocol: 快速, 信頼性, 堅牢性 🚀📦 [ID:7` | PASS |
| 8 | `normal` | 98 | 130 | 238 | No | No | `Record 8: packet monotonic transport monotoni` | PASS |
| 9 | `large` | 716 | 747 | 855 | No | No | `Record 9 LARGE_PAYLOAD: ABCDEF0123456789-ABCD` | PASS |
| 10 | `boundary` | 962 | 996 | 1106 | No | No | `Record 10 BOUNDARY_MAX_SIZE: XXXXXXXXXXXXXXXX` | PASS |
| 11 | `empty` | 0 | 31 | 141 | No | Yes | `` | PASS |
| 12 | `normal` | 104 | 136 | 246 | No | No | `Record 12: session recovery network acknowled` | PASS |
| 13 | `unicode` | 98 | 140 | 250 | No | No | `Special delimiters & escaped JSON quotes: {"k` | PASS |
| 14 | `boundary` | 962 | 996 | 1106 | No | No | `Record 14 BOUNDARY_MAX_SIZE: XXXXXXXXXXXXXXXX` | PASS |
| 15 | `large` | 586 | 617 | 727 | No | No | `Record 15 LARGE_PAYLOAD: ABCDEF0123456789-ABC` | PASS |
| 16 | `normal` | 72 | 104 | 214 | No | No | `Record 16: latency acknowledgment reordering ` | PASS |
| 17 | `unicode` | 103 | 136 | 246 | Yes | No | `Multi-lingual test: Здравствуйте мир! مرحبا ب` | PASS |
| 18 | `normal` | 125 | 157 | 267 | No | No | `Record 18: recovery transmission latency stre` | PASS |
| 19 | `empty` | 0 | 31 | 141 | No | Yes | `` | PASS |
| 20 | `boundary` | 962 | 996 | 1106 | No | No | `Record 20 BOUNDARY_MAX_SIZE: XXXXXXXXXXXXXXXX` | PASS |
| 21 | `normal` | 75 | 107 | 217 | No | No | `Record 21: window session recovery acknowledg` | PASS |
| 22 | `normal` | 69 | 101 | 211 | No | No | `Record 22: acknowledgment session reordering ` | PASS |
| 23 | `normal` | 63 | 95 | 205 | No | No | `Record 23: delivery packet network datagram t` | PASS |
| 24 | `normal` | 53 | 85 | 195 | No | No | `Record 24: latency session buffer reordering ` | PASS |
| 25 | `boundary` | 962 | 996 | 1106 | No | No | `Record 25 BOUNDARY_MAX_SIZE: XXXXXXXXXXXXXXXX` | PASS |
*(Showing first 25 samples above. See `reports/first_100_records_inspection.md` for the complete 100-record individual table.)*
