# Open311 Bronze ETL

## Purpose

This step converts one raw Open311 batch into flattened bronze JSONL records.

## Input

A raw batch directory containing:

- `response.json`
- `manifest.json`

## Output

Partitioned bronze JSONL output under:

```text
etl/open311/data/bronze/year=YYYY/month=MM/day=DD/city=<city>/batch_id=<batch_id>/part-00001.jsonl
