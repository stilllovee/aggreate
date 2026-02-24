## ChatGPT
```
How does an RDBMS SQL executor process GROUP BY and TOP queries internally?
```

Review the response from ChatGPT and create a guide for AI agents to execute (read the next section)

## Codex AGENT
For implementing:
```
requirement: <read in README.md>

guide: Execution Strategy Selection for ~10 Million Records
1. Assumptions for Decision Making

Reasonable real-world assumptions:

~10 million CSV rows

campaign_id cardinality is moderate (tens of thousands, at most a few hundred thousand)

Execution environment:

8–16 GB RAM

SSD storage

If campaign_id cardinality is close to the number of rows, the strategy changes (covered later).

2. Chosen Overall Strategy

Single-pass streaming scan + in-memory hash aggregation + Top-K heap

Rationale: fastest, simplest, no full sort required.

3. Phase-by-Phase Breakdown
3.1 Scan Phase: Streaming Input

Read the CSV file line by line or via buffered streaming

Do not load the entire file into memory

Parse each row and process immediately

Cost

Time: O(n)

Memory: small fixed buffer

3.2 GROUP BY Phase: Hash Aggregation

Use an in-memory structure:

Map<campaign_id, accumulator>

Each accumulator stores:

total_impressions

total_clicks

total_spend

total_conversions

Cost

Time: O(n)

Memory: O(number of campaigns)

For example:

100k campaigns

~64–100 bytes per entry

→ ~6–10 MB RAM, which is well within limits.

👉 No disk spilling required.

3.3 Metric Computation

CTR and CPA are computed after aggregation

Not calculated per input row

3.4 Top 10 by CTR

Iterate over aggregated results

Maintain a min-heap of size 10

Compare by CTR

Cost

Time: O(campaign_count × log 10) ≈ O(campaign_count)

Memory: negligible

3.5 Top 10 by CPA

Exclude campaigns with conversions = 0

Use the same Top-K heap approach

4. Why Not Sort-Based Strategies

Sort-based aggregation or full sorting:

Sorting 10M rows → O(n log n)

Heavy disk I/O

Only reasonable if:

Memory is extremely constrained, or

Input data is already sorted by campaign_id

👉 For 10M records, hash aggregation clearly wins.

5. When the Strategy Changes
5.1 Extremely High Cardinality

Example:

10M rows

~9–10M distinct campaign_ids

→ Hash map size approaches input size → high memory pressure.

In this case:

Use sort-based streaming aggregation, or

Hash partitioning with disk spill

5.2 Very Limited Memory (≤ 2GB)

In-memory hash table may cause OOM

Switch to:

Chunk-based processing with temporary files

External sort

6. Final Decision Summary

For ~10 million records in this challenge:

✅ Streaming scan + in-memory hash aggregation + Top-K heap

This approach:

Matches how SQL engines execute similar queries

Aligns with reviewer expectations

Minimizes complexity and bug risk.

please execute the requirement following by guide
```
For testing:
```
i need create a simple csvs and excecute code, not only test the logic
```