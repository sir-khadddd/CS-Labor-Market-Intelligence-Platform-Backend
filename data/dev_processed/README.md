# Dev Processed Snapshot

This folder contains a GitHub-safe, aggregated development snapshot for local bootstrapping.

- No raw posting rows.
- No individual-level records.
- Aggregated monthly facts and trajectory outputs only.
- Intended for local development and CI smoke tests.

To regenerate from local processed outputs:

```bash
python scripts/make_dev_snapshot.py
```
