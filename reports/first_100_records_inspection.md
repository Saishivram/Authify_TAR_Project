# TAR First 100 Records Individual Inspection Evidence

**Source Dataset:** `data/records_2000.jsonl`  
**Inspected Records:** Exactly 100 records (IDs 1 through 100)  
**Integrity Status:** [PASS] All 100 IDs Sequential & Valid  

## 1. Inspection Purpose and Protocol Context
This document provides complete, line-by-line inspection evidence for the first 100 logical records 
generated for the TAR (Time-Aware Reliable Transport) system. Every record has been verified against schema constraints, 
UTF-8 serialization rules, and payload size bounds.

### Size Metrics Reference
- **`Value Bytes`**: Byte length of raw application text (`payload['value']`).
- **`Serialized Bytes`**: Byte length of compact UTF-8 JSON object (`payload`).
- **`Datagram Bytes`**: Projected byte length when wrapped in the TAR DATA datagram envelope (must not exceed 1,200 bytes MTU).

### Empty Payload Note
For empty records (`is_empty = Yes`), `Value Bytes = 0`, while `Serialized Bytes = 31` due to JSON object keys and quotes.

## 2. Complete 100-Record Inspection Table

| ID | Category | Value Bytes | Serialized Bytes | Datagram Bytes | Unicode | Empty | Status | Preview |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | `normal` | 144 | 176 | 284 | No | No | PASS | `Record 1: datagram recovery acknowledgment da` |
| 2 | `empty` | 0 | 31 | 139 | No | Yes | PASS | `` |
| 3 | `unicode` | 97 | 139 | 247 | No | No | PASS | `Special delimiters & escaped JSON quotes: {"k` |
| 4 | `large` | 679 | 710 | 818 | No | No | PASS | `Record 4 LARGE_PAYLOAD: ABCDEF0123456789-ABCD` |
| 5 | `boundary` | 962 | 996 | 1104 | No | No | PASS | `Record 5 BOUNDARY_MAX_SIZE: XXXXXXXXXXXXXXXXX` |
| 6 | `normal` | 106 | 138 | 246 | No | No | PASS | `Record 6: acknowledgment session monotonic ne` |
| 7 | `unicode` | 68 | 101 | 209 | Yes | No | PASS | `TAR transport protocol: 快速, 信頼性, 堅牢性 🚀📦 [ID:7` |
| 8 | `normal` | 98 | 130 | 238 | No | No | PASS | `Record 8: packet monotonic transport monotoni` |
| 9 | `large` | 716 | 747 | 855 | No | No | PASS | `Record 9 LARGE_PAYLOAD: ABCDEF0123456789-ABCD` |
| 10 | `boundary` | 962 | 996 | 1106 | No | No | PASS | `Record 10 BOUNDARY_MAX_SIZE: XXXXXXXXXXXXXXXX` |
| 11 | `empty` | 0 | 31 | 141 | No | Yes | PASS | `` |
| 12 | `normal` | 104 | 136 | 246 | No | No | PASS | `Record 12: session recovery network acknowled` |
| 13 | `unicode` | 98 | 140 | 250 | No | No | PASS | `Special delimiters & escaped JSON quotes: {"k` |
| 14 | `boundary` | 962 | 996 | 1106 | No | No | PASS | `Record 14 BOUNDARY_MAX_SIZE: XXXXXXXXXXXXXXXX` |
| 15 | `large` | 586 | 617 | 727 | No | No | PASS | `Record 15 LARGE_PAYLOAD: ABCDEF0123456789-ABC` |
| 16 | `normal` | 72 | 104 | 214 | No | No | PASS | `Record 16: latency acknowledgment reordering ` |
| 17 | `unicode` | 103 | 136 | 246 | Yes | No | PASS | `Multi-lingual test: Здравствуйте мир! مرحبا ب` |
| 18 | `normal` | 125 | 157 | 267 | No | No | PASS | `Record 18: recovery transmission latency stre` |
| 19 | `empty` | 0 | 31 | 141 | No | Yes | PASS | `` |
| 20 | `boundary` | 962 | 996 | 1106 | No | No | PASS | `Record 20 BOUNDARY_MAX_SIZE: XXXXXXXXXXXXXXXX` |
| 21 | `normal` | 75 | 107 | 217 | No | No | PASS | `Record 21: window session recovery acknowledg` |
| 22 | `normal` | 69 | 101 | 211 | No | No | PASS | `Record 22: acknowledgment session reordering ` |
| 23 | `normal` | 63 | 95 | 205 | No | No | PASS | `Record 23: delivery packet network datagram t` |
| 24 | `normal` | 53 | 85 | 195 | No | No | PASS | `Record 24: latency session buffer reordering ` |
| 25 | `boundary` | 962 | 996 | 1106 | No | No | PASS | `Record 25 BOUNDARY_MAX_SIZE: XXXXXXXXXXXXXXXX` |
| 26 | `normal` | 59 | 91 | 201 | No | No | PASS | `Record 26: protocol delivery reordering trans` |
| 27 | `normal` | 113 | 145 | 255 | No | No | PASS | `Record 27: network recovery recovery stream s` |
| 28 | `normal` | 62 | 94 | 204 | No | No | PASS | `Record 28: delivery sequence delivery network` |
| 29 | `boundary` | 962 | 996 | 1106 | No | No | PASS | `Record 29 BOUNDARY_MAX_SIZE: XXXXXXXXXXXXXXXX` |
| 30 | `large` | 557 | 588 | 698 | No | No | PASS | `Record 30 LARGE_PAYLOAD: ABCDEF0123456789-ABC` |
| 31 | `normal` | 65 | 97 | 207 | No | No | PASS | `Record 31: sliding window packet buffer packe` |
| 32 | `normal` | 108 | 140 | 250 | No | No | PASS | `Record 32: window transmission delivery trans` |
| 33 | `normal` | 111 | 143 | 253 | No | No | PASS | `Record 33: buffer acknowledgment packet netwo` |
| 34 | `normal` | 85 | 117 | 227 | No | No | PASS | `Record 34: protocol acknowledgment datagram w` |
| 35 | `normal` | 54 | 86 | 196 | No | No | PASS | `Record 35: protocol latency throughput slidin` |
| 36 | `normal` | 155 | 187 | 297 | No | No | PASS | `Record 36: packet throughput timeout sliding ` |
| 37 | `normal` | 124 | 156 | 266 | No | No | PASS | `Record 37: monotonic stream window monotonic ` |
| 38 | `unicode` | 93 | 126 | 236 | Yes | No | PASS | `Mathematical symbols: ∀x ∈ ℝ, ∃y : f(x) ≤ ∫[0` |
| 39 | `normal` | 144 | 176 | 286 | No | No | PASS | `Record 39: stream stream session recovery tra` |
| 40 | `empty` | 0 | 31 | 141 | No | Yes | PASS | `` |
| 41 | `normal` | 59 | 91 | 201 | No | No | PASS | `Record 41: packet packet sliding throughput w` |
| 42 | `normal` | 49 | 81 | 191 | No | No | PASS | `Record 42: transport throughput session deliv` |
| 43 | `boundary` | 962 | 996 | 1106 | No | No | PASS | `Record 43 BOUNDARY_MAX_SIZE: XXXXXXXXXXXXXXXX` |
| 44 | `normal` | 138 | 170 | 280 | No | No | PASS | `Record 44: timeout reordering datagram transp` |
| 45 | `normal` | 77 | 109 | 219 | No | No | PASS | `Record 45: latency transmission monotonic rec` |
| 46 | `normal` | 128 | 160 | 270 | No | No | PASS | `Record 46: reordering protocol window datagra` |
| 47 | `normal` | 112 | 144 | 254 | No | No | PASS | `Record 47: reordering window packet transport` |
| 48 | `unicode` | 83 | 116 | 226 | Yes | No | PASS | `Emoji sequence: 🛰️ 📡 🌐 ⚡ 🛡️ ⏱️ 🎯 🔄 🚨 ✅ ❌ [ID:` |
| 49 | `normal` | 52 | 84 | 194 | No | No | PASS | `Record 49: window protocol datagram network l` |
| 50 | `normal` | 109 | 141 | 251 | No | No | PASS | `Record 50: datagram timeout sliding sequence ` |
| 51 | `large` | 608 | 639 | 749 | No | No | PASS | `Record 51 LARGE_PAYLOAD: ABCDEF0123456789-ABC` |
| 52 | `empty` | 0 | 31 | 141 | No | Yes | PASS | `` |
| 53 | `unicode` | 103 | 136 | 246 | Yes | No | PASS | `Multi-lingual test: Здравствуйте мир! مرحبا ب` |
| 54 | `large` | 490 | 521 | 631 | No | No | PASS | `Record 54 LARGE_PAYLOAD: ABCDEF0123456789-ABC` |
| 55 | `large` | 412 | 443 | 553 | No | No | PASS | `Record 55 LARGE_PAYLOAD: ABCDEF0123456789-ABC` |
| 56 | `normal` | 102 | 134 | 244 | No | No | PASS | `Record 56: sequence throughput protocol recov` |
| 57 | `normal` | 81 | 113 | 223 | No | No | PASS | `Record 57: monotonic buffer timeout sequence ` |
| 58 | `normal` | 54 | 86 | 196 | No | No | PASS | `Record 58: stream reordering transport latenc` |
| 59 | `normal` | 47 | 79 | 189 | No | No | PASS | `Record 59: datagram stream transmission strea` |
| 60 | `normal` | 54 | 86 | 196 | No | No | PASS | `Record 60: session timeout sequence throughpu` |
| 61 | `normal` | 135 | 167 | 277 | No | No | PASS | `Record 61: transmission network throughput de` |
| 62 | `normal` | 58 | 90 | 200 | No | No | PASS | `Record 62: recovery timeout timeout throughpu` |
| 63 | `normal` | 71 | 103 | 213 | No | No | PASS | `Record 63: transmission protocol latency reco` |
| 64 | `normal` | 53 | 85 | 195 | No | No | PASS | `Record 64: timeout acknowledgment sequence tr` |
| 65 | `normal` | 117 | 149 | 259 | No | No | PASS | `Record 65: throughput acknowledgment buffer w` |
| 66 | `unicode` | 98 | 140 | 250 | No | No | PASS | `Special delimiters & escaped JSON quotes: {"k` |
| 67 | `normal` | 69 | 101 | 211 | No | No | PASS | `Record 67: datagram network stream transport ` |
| 68 | `normal` | 158 | 190 | 300 | No | No | PASS | `Record 68: protocol buffer acknowledgment pro` |
| 69 | `normal` | 105 | 137 | 247 | No | No | PASS | `Record 69: protocol stream session stream sli` |
| 70 | `unicode` | 93 | 126 | 236 | Yes | No | PASS | `Mathematical symbols: ∀x ∈ ℝ, ∃y : f(x) ≤ ∫[0` |
| 71 | `normal` | 83 | 115 | 225 | No | No | PASS | `Record 71: monotonic reordering sequence buff` |
| 72 | `normal` | 97 | 129 | 239 | No | No | PASS | `Record 72: timeout delivery datagram acknowle` |
| 73 | `normal` | 69 | 101 | 211 | No | No | PASS | `Record 73: monotonic latency sequence window ` |
| 74 | `normal` | 90 | 122 | 232 | No | No | PASS | `Record 74: reordering latency sliding monoton` |
| 75 | `normal` | 110 | 142 | 252 | No | No | PASS | `Record 75: network timeout protocol throughpu` |
| 76 | `normal` | 140 | 172 | 282 | No | No | PASS | `Record 76: throughput sliding transmission re` |
| 77 | `normal` | 106 | 138 | 248 | No | No | PASS | `Record 77: acknowledgment packet stream netwo` |
| 78 | `normal` | 65 | 97 | 207 | No | No | PASS | `Record 78: session acknowledgment protocol se` |
| 79 | `normal` | 150 | 182 | 292 | No | No | PASS | `Record 79: recovery sequence transmission tim` |
| 80 | `normal` | 136 | 168 | 278 | No | No | PASS | `Record 80: transport datagram stream sequence` |
| 81 | `large` | 478 | 509 | 619 | No | No | PASS | `Record 81 LARGE_PAYLOAD: ABCDEF0123456789-ABC` |
| 82 | `normal` | 145 | 177 | 287 | No | No | PASS | `Record 82: delivery protocol transport window` |
| 83 | `normal` | 100 | 132 | 242 | No | No | PASS | `Record 83: network sliding transmission ackno` |
| 84 | `normal` | 69 | 101 | 211 | No | No | PASS | `Record 84: packet recovery buffer sequence pr` |
| 85 | `unicode` | 69 | 102 | 212 | Yes | No | PASS | `TAR transport protocol: 快速, 信頼性, 堅牢性 🚀📦 [ID:8` |
| 86 | `normal` | 100 | 132 | 242 | No | No | PASS | `Record 86: protocol timeout session buffer de` |
| 87 | `normal` | 53 | 85 | 195 | No | No | PASS | `Record 87: recovery timeout recovery stream p` |
| 88 | `normal` | 156 | 188 | 298 | No | No | PASS | `Record 88: buffer recovery stream delivery re` |
| 89 | `normal` | 103 | 135 | 245 | No | No | PASS | `Record 89: datagram network reordering buffer` |
| 90 | `normal` | 137 | 169 | 279 | No | No | PASS | `Record 90: packet datagram session timeout st` |
| 91 | `empty` | 0 | 31 | 141 | No | Yes | PASS | `` |
| 92 | `normal` | 132 | 164 | 274 | No | No | PASS | `Record 92: packet stream packet sequence tran` |
| 93 | `normal` | 80 | 112 | 222 | No | No | PASS | `Record 93: recovery recovery buffer protocol ` |
| 94 | `boundary` | 962 | 996 | 1106 | No | No | PASS | `Record 94 BOUNDARY_MAX_SIZE: XXXXXXXXXXXXXXXX` |
| 95 | `normal` | 122 | 154 | 264 | No | No | PASS | `Record 95: acknowledgment datagram reordering` |
| 96 | `normal` | 79 | 111 | 221 | No | No | PASS | `Record 96: delivery buffer packet network thr` |
| 97 | `empty` | 0 | 31 | 141 | No | Yes | PASS | `` |
| 98 | `large` | 694 | 725 | 835 | No | No | PASS | `Record 98 LARGE_PAYLOAD: ABCDEF0123456789-ABC` |
| 99 | `normal` | 46 | 78 | 188 | No | No | PASS | `Record 99: stream protocol reordering network` |
| 100 | `unicode` | 94 | 127 | 239 | Yes | No | PASS | `Mathematical symbols: ∀x ∈ ℝ, ∃y : f(x) ≤ ∫[0` |

## 3. Inspection Verification Findings
- **Sequential IDs:** Confirmed strictly sequential from 1 to 100 with zero duplicates and zero gaps.
- **Edge Cases Exercised:** Normal, empty (0-byte value), Unicode (multi-byte UTF-8 scripts/emojis), large (400–800 bytes), and boundary (~996 bytes) payloads are all represented.
- **Wire Budget Compliance:** Maximum estimated datagram size is well within the 1,200-byte UDP MTU ceiling.
- **Serializer Status:** Phase 1 uses estimated DATA datagram sizing. Actual live datagram serialization tests will execute in Phase 2/3.
