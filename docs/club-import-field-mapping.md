# Club Import Field Mapping

This spec is based on the current GeoDirectory exports:

- `/Users/paul/Downloads/gd_place_1404262014_66624f6f.csv`
- `/Users/paul/Downloads/gd_placecategory_1404262017_b20b76e1.csv`

## Current GeoDirectory Shape

- Listing post type: `gd_place`
- Listing rows exported: `21`
- Category rows exported: `9`
- Current audience model: stored in `post_tags`
- Current discipline model: stored in `post_category` and `default_category`
- Current image model:
  - `logo` = single image field
  - `post_images` = gallery field

Current category IDs:

| ID | Name |
| --- | --- |
| 40 | Uncategorized |
| 41 | MTB |
| 42 | Road |
| 43 | Gravel |
| 48 | MTB XC |
| 49 | Track |
| 50 | Touring |
| 51 | Cyclo-cross |
| 57 | Social |

Important import conventions observed in your export:

- `post_category` uses comma-wrapped IDs, for example `,43,42,49,`
- `default_category` uses a single category ID, for example `42`
- `logo` uses one image entry in the form `image_url|attachment_id|title|caption`
- `post_images` uses one or more image entries joined by `::`
- `post_tags` is currently either `Women Only` or `Mixed Gender`

## Recommended Workflow

1. Discover candidate clubs outside WordPress.
2. Store candidates in a staging sheet or database.
3. Review the records for quality, duplicates, and image rights.
4. Match staged rows against an export of your live GeoDirectory listings and fill `existing_gd_id` where a club already exists.
5. Generate a GeoDirectory-ready CSV using the exact `gd_place` headers.
6. Import into a staging copy of WordPress first.
7. Publish to production only after spot-checking layouts and maps.

## Staging Schema

Use the staging template at `/Users/paul/Documents/VDM/CFW_Clubs/templates/club-staging-template.csv`.

Core staging rules:

- Every row needs a stable `external_id`.
- Never publish directly from scraped results.
- Keep `summary_raw` and `summary_final` separate.
- Keep image source URLs and rights status separate from final import columns.
- Preserve `existing_gd_id` when matching an existing listing.

Suggested review statuses:

- `pending`
- `approved`
- `needs_research`
- `rejected`
- `published`

Suggested image rights statuses:

- `official-site`
- `official-social`
- `club-provided`
- `licensed`
- `unknown`
- `do-not-import`

## Field Mapping

| GeoDirectory field | Populate from staging | Automation rule | Review level | Notes |
| --- | --- | --- | --- | --- |
| `ID` | `existing_gd_id` | Blank for new clubs, existing GD post ID for updates | Medium | Use only after dedupe confirms a match |
| `post_title` | `club_name` | Copy as final cleaned club name | Medium | Normalize punctuation and spacing |
| `post_content` | `summary_final` | Use edited short description, not scraped long copy | High | Keep concise and original |
| `post_status` | fixed value | Set to `draft` for first import batch, then `publish` after QA | Low | Safer rollout |
| `post_author` | fixed value | Default to your admin/import user ID | Low | Current export uses `1` |
| `post_type` | fixed value | Always `gd_place` unless you create a new club CPT | Low | Match current site |
| `post_date` | generated | Use current timestamp for new rows | Low | Optional on import if GD fills it |
| `post_modified` | generated | Use current timestamp on export generation | Low | Optional |
| `post_tags` | `audience` | Map to `Women Only` or `Mixed Gender` | Medium | Later this should become a dedicated field |
| `post_category` | `disciplines_csv` | Convert discipline names to comma-wrapped category IDs | High | Example: `Road,Gravel` -> `,42,43,` |
| `default_category` | `discipline_primary` | Convert primary discipline to one category ID | High | Required for consistency |
| `featured` | fixed value | Default `0` | Low | Set `1` only for editorial picks |
| `street` | `street` or `plus_code` | Use exact street when known, otherwise a meetup label or plus code | Medium | Avoid fake precision |
| `street2` | `street2` | Optional | Low | Usually blank |
| `city` | `city` | Required minimum location field | High | Normalize names |
| `region` | `region` | Required minimum location field | High | Example `Ontario` |
| `country` | `country` | Required minimum location field | High | Use full country name as in current export |
| `zip` | `postal_code` | Optional | Medium | Keep blank if unknown |
| `latitude` | `latitude` | Prefer verified geocoded value | High | Needed for maps |
| `longitude` | `longitude` | Prefer verified geocoded value | High | Needed for maps |
| `logo` | `logo_import_value` | Use approved official logo URL | High | Leave blank if rights are unclear |
| `website` | `website` | Official club site preferred | High | Key dedupe field |
| `twitter` | `twitter_url` | Full URL if known | Medium | Leave blank if only uncertain handle exists |
| `twitterusername` | `twitter_handle` | Clean handle without `@` | Medium | Optional if platform is inactive |
| `facebook` | `facebook_url` | Full URL | Medium | |
| `instagram` | `instagram_url` | Full URL | Medium | |
| `video` | `youtube_url` | Optional | Low | Use official channel/video only |
| `strava` | `strava_url` | Optional | Low | Rare in current export |
| `focus` | `focus` | Controlled vocabulary if you keep it | Medium | Current values are sparse; may be better removed or redesigned |
| `email` | `email` | Only use public contact email from official source | High | Currently unused |
| `post_images` | `gallery_import_value` | Use `cover_source_url` plus optional `gallery_source_urls` for 1 to 3 approved images max | High | First image acts as the lead visual in many layouts |

## Discipline Mapping

Map staged discipline names to the current category IDs:

| Discipline | GD category ID |
| --- | --- |
| `MTB` | `41` |
| `Road` | `42` |
| `Gravel` | `43` |
| `MTB XC` | `48` |
| `Track` | `49` |
| `Touring` | `50` |
| `Cyclo-cross` | `51` |
| `Social` | `57` |

Transformation rules:

1. Remove blanks and duplicates.
2. If no discipline is available, fall back to `Social`.
3. Map names to IDs.
4. Sort IDs in a consistent order.
5. Wrap the joined list with leading and trailing commas.

Audience fallback rule:

- If `audience` is blank during export, default to `Mixed Gender`.

Example:

- Input disciplines: `Road, Gravel, Track`
- Output `post_category`: `,42,43,49,`
- Output `default_category`: `42`

## Image Rules

For MVP:

- Import logos only from official club websites, official socials, or club-provided files.
- Import cover/gallery images only if they are clearly club-owned or licensed.
- If rights are unclear, import the listing without those images.
- Limit galleries to 1 to 3 strong images.

CSV formatting rules from your current export shape:

- `logo` expects one image token
- `post_images` expects one or more image tokens joined by `::`
- A token looks like `image_url|attachment_id|title|caption`

For newly generated imports, the safest starting assumption is:

- `logo`: `https://example.com/logo.png|||`
- `post_images`: `https://example.com/cover.jpg|||::https://example.com/ride-2.jpg|||`

That keeps the URL and leaves attachment metadata blank for WordPress/GeoDirectory to resolve during import.

## Dedupe Rules

Match existing clubs before creating a new row using this priority:

1. Exact official website domain
2. Existing GeoDirectory `ID`
3. Exact Facebook or Instagram URL
4. Normalized club name plus city
5. Manual review

When matched:

- Keep the existing `ID`
- Update only approved fields
- Do not overwrite manually curated text or images unless explicitly allowed

## Pre-Import Matcher

Use the matcher script at `/Users/paul/Documents/VDM/CFW_Clubs/scripts/match_existing_geodirectory_ids.py`.

What it does:

- compares staged clubs to your existing GeoDirectory export
- fills `existing_gd_id` for safe one-to-one matches
- prefers exact website domain matches
- falls back to exact social URL matches
- then falls back to normalized club name plus city/region/country
- leaves ambiguous rows blank for manual review

Example:

```bash
python3 /Users/paul/Documents/VDM/CFW_Clubs/scripts/match_existing_geodirectory_ids.py \
  /Users/paul/Documents/VDM/CFW_Clubs/data/club-staging-openai-pilot.csv \
  /Users/paul/Downloads/gd_place_1404262014_66624f6f.csv \
  /Users/paul/Documents/VDM/CFW_Clubs/data/club-staging-openai-pilot-matched.csv
```

After matching:

- use the matched staging CSV as the input to `staging_to_geodirectory.py`
- import in GeoDirectory with the option to update listings when `ID` already exists

## Publishing Rules

Recommended rollout:

1. Generate GeoDirectory CSV from approved staging rows only.
2. Import into a staging site as `draft`.
3. Review map pins, archive cards, Elementor single pages, and image rendering.
4. Re-import updates with `ID` filled for matched rows.
5. Move production imports to `publish` only after the batch looks stable.

## Converter Script

Use the converter script at `/Users/paul/Documents/VDM/CFW_Clubs/scripts/staging_to_geodirectory.py`.

Example:

```bash
python3 /Users/paul/Documents/VDM/CFW_Clubs/scripts/staging_to_geodirectory.py \
  /Users/paul/Documents/VDM/CFW_Clubs/examples/club-staging-sample.csv \
  /Users/paul/Documents/VDM/CFW_Clubs/examples/generated-gd_place-import-sample.csv
```

Default behavior:

- only exports rows with `review_status` of `approved` or `published`
- maps discipline names to your current GeoDirectory category IDs
- falls back to `Social` (`57`) when both discipline fields are blank
- falls back to `Mixed Gender` when `audience` is blank
- writes `post_status=draft`
- uses `discovered_at` and `last_checked` for post dates when available
- only includes logo/gallery images when the rights status is approved
- falls back from `summary_final` to `summary_raw` if needed

## First Automation Scope

Automate first:

- club name
- website
- city
- region
- country
- coordinates
- disciplines
- audience
- Facebook
- Instagram
- Twitter
- YouTube
- Strava
- official logo URL

Keep human-reviewed for MVP:

- final summary text
- `focus`
- gallery image selection
- email
- questionable locations
- any club that looks like a shop ride, one-off event, or unofficial group

## Recommended Near-Term Improvements

- Add a dedicated audience field instead of relying on `post_tags`
- Add a source URL field for editorial traceability
- Add an internal review status outside GeoDirectory
- Add a manual lock flag so imports do not overwrite curated records
- Consider a dedicated `club` CPT later if you want club-specific fields and filters

## Reference Links

- GeoDirectory import/export: <https://wpgeodirectory.com/docs-v2/geodirectory/settings/import-export/>
- GeoDirectory import/export how-to: <https://wpgeodirectory.com/documentation/article/how-tos/how-to-import-export-data/>
- WordPress media sideloading: <https://developer.wordpress.org/reference/functions/media_sideload_image/>
