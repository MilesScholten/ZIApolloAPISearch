# Company Enrichment (ZoomInfo + Apollo)

A modular toolkit and Streamlit app to enrich a CSV of companies using **ZoomInfo** and **Apollo**, with matching priorities:

- **ZoomInfo:** ZoomInfo Company ID → Domain → Name
- **Apollo:** Apollo Company ID → Salesforce Account ID → Domain → Name

Includes:
- **Streamlit GUI** with picklists for input mapping and a JSON-based output rename tool.
- **CLI** for batch enrichment with YAML config.
- **Configurable endpoints**, rate limits, retries, and field prefixes.
- **Optional Salesforce join** to backfill domains from a Salesforce Accounts CSV.
- **Unit tests** with stubbed clients.
- **Dockerfile** and **Makefile** for easy setup.

## Quickstart

```bash
# 1) Clone & set up
pip install -r requirements.txt

# 2) (optional) export keys
export ZOOMINFO_API_KEY="your_zoominfo_key"
export APOLLO_API_KEY="your_apollo_key"

# 3) Run Streamlit app
streamlit run app/streamlit_app.py
```

Or use the CLI:

```bash
python scripts/enrich_cli.py -i sample_data/input.sample.csv -o out.csv -c config/config.example.yaml
```

## Configuration

See `config/config.example.yaml` for API endpoints, rate limits, output prefixes, retry policy, and input field mapping.

## Testing

```bash
pytest -q
```

## Docker

```bash
docker build -t enrichment .
docker run --rm -p 8501:8501 -e ZOOMINFO_API_KEY -e APOLLO_API_KEY enrichment
# then open http://localhost:8501
```

## Notes

- Endpoints and payload shapes vary by account/plan; tweak in the YAML or client classes.
- No real API calls are executed in tests; they use stub clients.
- For large files, consider setting stricter rate limits, or add concurrency via batch queues if your plan allows.
