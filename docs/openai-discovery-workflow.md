# OpenAI Discovery Workflow

This workflow uses the OpenAI Responses API with the `web_search` tool and Structured Outputs to discover cycling club candidates with much tighter filtering than keyword-only search.

## Why Use This Path

Use this when simple search results are too noisy.

The OpenAI-based workflow:

1. Expands a province/state or country into city seeds using GeoNames.
2. Runs one discovery pass per city with OpenAI web search.
3. Returns structured candidate JSON instead of loose text.
4. Optionally runs a second verification pass restricted to the candidate's official domain.
5. Enriches the official website with deterministic metadata extraction.
6. Writes the result into the same staging CSV used by the GeoDirectory converter.

## Files

- GeoNames seed generator: `/Users/paul/Documents/VDM/CFW_Clubs/scripts/generate_location_seeds_from_geonames.py`
- OpenAI discovery script: `/Users/paul/Documents/VDM/CFW_Clubs/scripts/discover_clubs_with_openai.py`
- Summary backfill script: `/Users/paul/Documents/VDM/CFW_Clubs/scripts/backfill_summary_final.py`
- Image cleanup script: `/Users/paul/Documents/VDM/CFW_Clubs/scripts/sanitize_staging_images.py`
- GeoDirectory converter: `/Users/paul/Documents/VDM/CFW_Clubs/scripts/staging_to_geodirectory.py`

## Credentials

Set your OpenAI API key:

```bash
export OPENAI_API_KEY="your-api-key"
```

## Example Run

Generate Ontario city seeds:

```bash
cd /Users/paul/Documents/VDM/CFW_Clubs

python3 scripts/generate_location_seeds_from_geonames.py \
  geonames/cities15000.zip \
  data/ontario-location-seeds.csv \
  --country-code CA \
  --admin1 Ontario \
  --admin1-codes-file geonames/admin1CodesASCII.txt \
  --country-info-file geonames/countryInfo.txt \
  --min-population 15000 \
  --max-locations 150
```

Run OpenAI discovery:

```bash
cd /Users/paul/Documents/VDM/CFW_Clubs

export OPENAI_API_KEY="your-api-key"

python3 scripts/discover_clubs_with_openai.py \
  data/ontario-location-seeds.csv \
  data/club-staging-openai.csv \
  --discovery-model gpt-5.4-mini \
  --verification-model gpt-5.4-mini \
  --max-locations 5 \
  --min-confidence 0.8 \
  --max-candidates-per-location 4 \
  --discovery-max-output-tokens 1400 \
  --verification-max-output-tokens 500 \
  --max-retries 6
```

Convert approved rows into a GeoDirectory import:

```bash
cd /Users/paul/Documents/VDM/CFW_Clubs

python3 scripts/staging_to_geodirectory.py \
  data/club-staging-openai.csv \
  data/gd_place-import-openai.csv
```

## Notes

- The script writes only `pending` rows. Review them before import.
- OpenAI discovery now writes polished editorial copy into `summary_final`, while keeping raw source text in `summary_raw`.
- Start with `--max-locations 3` to `5` for a cheap pilot before a full province run.
- Verification is on by default. Use `--skip-verification` only for faster experiments.
- `gpt-5.4-mini` is a good cost-quality starting point. If you want the strongest filtering, switch both model flags to `gpt-5.4`.
- If you hit OpenAI rate limits, lower `--max-candidates-per-location`, lower the output token caps, or simply rerun; the script now retries 429 responses automatically.
- If you have older staging files with blank or rough `summary_final` values, you can backfill them locally:
- If you have older staging files with mixed-up image fields, you can clean them locally so logos stay in `logo_source_url` and club photos stay in `cover_source_url` / `gallery_source_urls`:

```bash
cd /Users/paul/Documents/VDM/CFW_Clubs

python3 scripts/sanitize_staging_images.py \
  data/club-staging-openai-pilot.csv \
  data/club-staging-openai-pilot.csv
```

- If you have older staging files with blank or rough `summary_final` values, you can backfill them locally:

```bash
cd /Users/paul/Documents/VDM/CFW_Clubs

python3 scripts/backfill_summary_final.py \
  data/club-staging-openai-pilot.csv \
  data/club-staging-openai-pilot.csv \
  --replace-existing
```

## Official References

- OpenAI web search guide: <https://platform.openai.com/docs/guides/tools-web-search>
- OpenAI Responses API: <https://platform.openai.com/docs/api-reference/responses>
- OpenAI Structured Outputs guide: <https://platform.openai.com/docs/guides/structured-outputs>
