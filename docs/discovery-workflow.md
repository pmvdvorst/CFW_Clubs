# Discovery Workflow

Use this workflow to discover candidate clubs by location and stage them for editorial review before import.

## Files

- Location seed template: `/Users/paul/Documents/VDM/CFW_Clubs/templates/location-seeds-template.csv`
- Sample location seeds: `/Users/paul/Documents/VDM/CFW_Clubs/examples/location-seeds-sample.csv`
- GeoNames seed generator: `/Users/paul/Documents/VDM/CFW_Clubs/scripts/generate_location_seeds_from_geonames.py`
- Discovery script: `/Users/paul/Documents/VDM/CFW_Clubs/scripts/discover_clubs.py`
- Staging template: `/Users/paul/Documents/VDM/CFW_Clubs/templates/club-staging-template.csv`
- GeoDirectory converter: `/Users/paul/Documents/VDM/CFW_Clubs/scripts/staging_to_geodirectory.py`

## Architecture

The clean region-scale workflow is:

1. Generate locality seeds from GeoNames for a province/state or country.
2. Run Brave Web Search and Brave Place Search for each locality.
3. Merge and score candidates.
4. Enrich candidates from official sites.
5. Review the staging CSV.
6. Convert approved rows into a GeoDirectory import CSV.

The scripts do not publish to WordPress directly.

## GeoNames Seed Generation

Use GeoNames city datasets such as `cities5000.zip` or `cities15000.zip`, plus:

- `countryInfo.txt`
- `admin1CodesASCII.txt`

Generate province/state seeds:

```bash
python3 /Users/paul/Documents/VDM/CFW_Clubs/scripts/generate_location_seeds_from_geonames.py \
  /path/to/cities15000.zip \
  /Users/paul/Documents/VDM/CFW_Clubs/data/ontario-location-seeds.csv \
  --country-code CA \
  --admin1 Ontario \
  --admin1-codes-file /path/to/admin1CodesASCII.txt \
  --country-info-file /path/to/countryInfo.txt \
  --min-population 15000 \
  --max-locations 150
```

Generate country-wide seeds:

```bash
python3 /Users/paul/Documents/VDM/CFW_Clubs/scripts/generate_location_seeds_from_geonames.py \
  /path/to/cities15000.zip \
  /Users/paul/Documents/VDM/CFW_Clubs/data/canada-location-seeds.csv \
  --country-code CA \
  --country-info-file /path/to/countryInfo.txt \
  --min-population 15000 \
  --max-locations 400
```

Official GeoNames download pages:

- <https://www.geonames.org/export>
- <https://download.geonames.org/export/dump/>

## Discovery

For each location seed, the discovery script:

1. Runs several Brave Web Search queries targeted to that city/region.
2. Runs several Brave Place Search queries around the same coordinates.
3. Scores likely cycling-club candidates.
4. Fetches candidate pages and extracts:
   - title and description
   - canonical URL
   - social links
   - email
   - JSON-LD address and coordinates when present
   - logo and cover image URLs from page metadata
5. Infers likely audience and disciplines.
6. Writes `pending` rows into the staging CSV.

Set this environment variable before running discovery:

```bash
export BRAVE_SEARCH_API_KEY="your-api-key"
```

The script uses the Brave Search API. Official docs:

- <https://brave.com/search/api/>
- <https://api-dashboard.search.brave.com/documentation/guides/authentication>
- <https://api-dashboard.search.brave.com/documentation/services/web-search>
- <https://api-dashboard.search.brave.com/documentation/services/place-search>

## Seed CSV Format

Headers:

```csv
location_id,location_name,city,region,country,postal_code,latitude,longitude,query_hint
```

Recommended usage:

- `location_id`: stable slug
- `location_name`: friendly display label
- `city`, `region`, `country`: required
- `latitude`, `longitude`: recommended for better Place Search targeting
- `query_hint`: optional text like `road gravel mtb`

## First Run

Use a small pilot first:

```bash
python3 /Users/paul/Documents/VDM/CFW_Clubs/scripts/discover_clubs.py \
  /Users/paul/Documents/VDM/CFW_Clubs/examples/location-seeds-sample.csv \
  /Users/paul/Documents/VDM/CFW_Clubs/data/club-staging.csv \
  --providers web,place
```

Then convert approved rows into a GeoDirectory import file:

```bash
python3 /Users/paul/Documents/VDM/CFW_Clubs/scripts/staging_to_geodirectory.py \
  /Users/paul/Documents/VDM/CFW_Clubs/data/club-staging.csv \
  /Users/paul/Documents/VDM/CFW_Clubs/data/gd_place-import.csv
```

## Province/State Or Country Run

```bash
python3 /Users/paul/Documents/VDM/CFW_Clubs/scripts/discover_clubs.py \
  /Users/paul/Documents/VDM/CFW_Clubs/data/ontario-location-seeds.csv \
  /Users/paul/Documents/VDM/CFW_Clubs/data/club-staging.csv \
  --providers web,place \
  --results-per-query 10 \
  --max-candidates-per-location 12 \
  --place-radius-meters 50000
```

## Important Flags

Discovery script:

- `--providers web,place`
- `--results-per-query 10`
- `--max-candidates-per-location 10`
- `--min-score 0.20`
- `--place-radius-meters 50000`
- `--replace-existing`

Converter script:

- `--post-status draft`
- `--strict`

## Editorial Guidance

Treat discovered rows as leads, not final listings.

Always review:

- final summary text
- audience classification
- discipline classification
- location accuracy
- logo and cover image rights
- whether the result is a real club vs a shop ride, event page, or one-off program

## Why This Structure

- GeoNames gives you free, clean locality expansion for province/state/country runs.
- Brave Web Search finds clubs that mainly live on websites or social profiles.
- Brave Place Search catches location-oriented entities you might miss from web queries alone.
- The staging CSV remains the review checkpoint before anything reaches WordPress.
- This architecture uses the same code path for city, province/state, and country discovery.

## Why Brave

Google changed Programmable Search on January 20, 2026 so that new search engines must use `Sites to search`, while older full-web engines can continue only until January 1, 2027.

Brave Search API is a practical replacement because it is still a whole-web search API. Brave also launched a dedicated Place Search endpoint on February 26, 2026, and this pipeline uses both.
