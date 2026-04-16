import unittest

from scripts.discover_clubs import (
    LocationSeed,
    SearchResult,
    apply_social_logo_fallback,
    build_queries,
    build_place_queries,
    build_staging_row,
    choose_club_name,
    choose_asset_images,
    extract_metadata,
    extract_metadata_with_asset_fallback,
    homepage_candidate_urls,
    sanitize_image_selection,
    metadata_from_place_result,
    infer_audience,
    infer_disciplines,
    place_location_string,
    score_result,
)


class DiscoverClubsTest(unittest.TestCase):
    def test_extract_metadata_reads_jsonld_and_social_links(self) -> None:
        html = """
        <html>
          <head>
            <title>Wild Bettys | Toronto Women's MTB</title>
            <meta name="description" content="A Toronto-based club for women mountain bikers.">
            <meta property="og:image" content="/images/hero.jpg">
            <link rel="canonical" href="https://wildbettys.com/">
            <script type="application/ld+json">
            {
              "@context": "https://schema.org",
              "@type": "SportsOrganization",
              "name": "Wild Bettys",
              "url": "https://wildbettys.com/",
              "logo": "https://wildbettys.com/images/logo.png",
              "sameAs": [
                "https://www.facebook.com/WildBettys/",
                "https://www.instagram.com/wildbettys/",
                "https://twitter.com/wildbettys"
              ],
              "address": {
                "@type": "PostalAddress",
                "streetAddress": "1191 Lawrence Avenue East",
                "addressLocality": "Toronto",
                "addressRegion": "Ontario",
                "postalCode": "M3A 3R3",
                "addressCountry": "Canada"
              },
              "geo": {
                "@type": "GeoCoordinates",
                "latitude": 43.7400452,
                "longitude": -79.328443
              }
            }
            </script>
          </head>
          <body>
            <a href="https://www.youtube.com/watch?v=2BcuqlUmrNM">YouTube</a>
            <a href="mailto:hello@wildbettys.com">Email</a>
          </body>
        </html>
        """
        metadata = extract_metadata(html, "https://wildbettys.com/")
        self.assertEqual(metadata["site_name"], "Wild Bettys")
        self.assertEqual(metadata["logo_url"], "https://wildbettys.com/images/logo.png")
        self.assertEqual(metadata["cover_url"], "https://wildbettys.com/images/hero.jpg")
        self.assertEqual(metadata["facebook_url"], "https://www.facebook.com/WildBettys")
        self.assertEqual(metadata["instagram_url"], "https://www.instagram.com/wildbettys")
        self.assertEqual(metadata["twitter_url"], "https://twitter.com/wildbettys")
        self.assertEqual(metadata["youtube_url"], "https://www.youtube.com/watch")
        self.assertEqual(metadata["email"], "hello@wildbettys.com")
        self.assertEqual(metadata["city"], "Toronto")
        self.assertEqual(metadata["region"], "Ontario")
        self.assertEqual(metadata["gallery_urls"], "")

    def test_extract_metadata_falls_back_to_img_heuristics(self) -> None:
        html = """
        <html>
          <head>
            <title>Club Example</title>
          </head>
          <body>
            <img src="/assets/club-logo.svg" alt="Club Example logo">
            <img src="/media/team-ride.jpg" alt="Women's team ride" width="1400" height="900">
            <img data-src="/media/gallery-2.webp" class="gallery-image" alt="Club group ride">
          </body>
        </html>
        """
        metadata = extract_metadata(html, "https://club.example/about")
        self.assertEqual(metadata["logo_url"], "https://club.example/assets/club-logo.svg")
        self.assertEqual(metadata["cover_url"], "https://club.example/media/team-ride.jpg")
        self.assertEqual(metadata["gallery_urls"], "")

    def test_extract_metadata_prefers_landscape_photo_over_portrait(self) -> None:
        html = """
        <html>
          <head>
            <title>Club Example</title>
          </head>
          <body>
            <img src="/media/group-portrait.jpg" alt="Women's group ride" width="700" height="1100">
            <img src="/media/group-landscape.jpg" alt="Women's group ride" width="1400" height="800">
          </body>
        </html>
        """
        metadata = extract_metadata(html, "https://club.example/")
        self.assertEqual(metadata["cover_url"], "https://club.example/media/group-landscape.jpg")
        self.assertEqual(metadata["gallery_urls"], "https://club.example/media/group-portrait.jpg")

    def test_extract_metadata_accepts_wix_asset_logo(self) -> None:
        html = """
        <html>
          <head><title>NCR Spoke Sisters</title></head>
          <body>
            <img
              src="https://static.wixstatic.com/media/e78584_b0dbcd3439f04273a72afbee70eed7eb~mv2.png/v1/fill/w_234,h_183,al_c,q_85/Spoke_Sisters_Logo.png"
              alt="Spoke Sisters"
              width="234"
              height="183"
            >
          </body>
        </html>
        """
        metadata = extract_metadata(html, "https://www.spokesisters.ca/about")
        self.assertEqual(
            metadata["logo_url"],
            "https://static.wixstatic.com/media/e78584_b0dbcd3439f04273a72afbee70eed7eb~mv2.png/v1/fill/w_234,h_183,al_c,q_85/Spoke_Sisters_Logo.png",
        )
        self.assertEqual(metadata["cover_url"], "")

    def test_extract_metadata_keeps_cover_when_filename_is_generic_but_alt_text_is_photo_like(self) -> None:
        html = """
        <html>
          <head><title>Women's Cycling Network</title></head>
          <body>
            <img src="/asset/image/site-logo.png" alt="Women's Cycling Network logo">
            <img
              src="/asset/image/ourwork-advocacy.jpeg"
              alt="A diverse group of women standing side by side and smiling at the camera"
              class="work-img"
            >
          </body>
        </html>
        """
        metadata = extract_metadata(html, "https://www.womenscyclingnetwork.ca/")
        self.assertEqual(
            metadata["cover_url"],
            "https://www.womenscyclingnetwork.ca/asset/image/ourwork-advocacy.jpeg",
        )

    def test_extract_metadata_filters_sponsor_tiles_when_duplicate_alt_is_present(self) -> None:
        html = """
        <html>
          <head><title>Common Empire</title></head>
          <body>
            <img
              data-stretch="true"
              data-src="https://images.squarespace-cdn.com/content/v1/site/Common-Empire-Rise-and-Ride4.jpeg"
              src="https://images.squarespace-cdn.com/content/v1/site/Common-Empire-Rise-and-Ride4.jpeg"
              width="5341"
              height="3310"
              alt="Group of cyclists riding on a bridge with city skyline and fog in the background."
              alt=""
            >
            <img
              data-stretch="false"
              data-src="https://images.squarespace-cdn.com/content/v1/site/Group+1.png"
              src="https://images.squarespace-cdn.com/content/v1/site/Group+1.png"
              width="387"
              height="197"
              alt="Salomon logo on a black background."
              alt=""
            >
          </body>
        </html>
        """
        metadata = extract_metadata(html, "https://www.commonempire.com/")
        self.assertEqual(
            metadata["cover_url"],
            "https://images.squarespace-cdn.com/content/v1/site/Common-Empire-Rise-and-Ride4.jpeg",
        )
        self.assertEqual(metadata["gallery_urls"], "")

    def test_extract_metadata_with_asset_fallback_reads_js_shell_assets(self) -> None:
        html = """
        <html>
          <head>
            <title>Saddle Sisters of High Park</title>
            <meta name="description" content="Saddle Sisters of High Park website">
            <link rel="stylesheet" href="/static/css/main.css">
            <script defer src="/static/js/main.js"></script>
          </head>
          <body>
            <noscript>You need to enable JavaScript to run this app.</noscript>
            <div id="root"></div>
          </body>
        </html>
        """
        resources = {
            "https://saddlesisters.ca/static/css/main.css": """
                .hero {
                    background-image: url('/static/media/saddle-sisters-banner.jpg');
                }
            """,
            "https://saddlesisters.ca/static/js/main.js": """
                const logo = "/static/media/saddle-sisters-logo.svg";
                const team = "sshp/saddle-sisters-team-photo.webp";
            """,
        }

        def fetcher(url: str, timeout: float, user_agent: str) -> str:
            del timeout, user_agent
            return resources[url]

        metadata = extract_metadata_with_asset_fallback(
            html,
            "https://saddlesisters.ca/",
            timeout=10.0,
            user_agent="CFW-Test/1.0",
            fetcher=fetcher,
        )
        self.assertEqual(
            metadata["logo_url"],
            "https://saddlesisters.ca/static/media/saddle-sisters-logo.svg",
        )
        self.assertEqual(
            metadata["cover_url"],
            "https://saddlesisters.ca/static/media/saddle-sisters-banner.jpg",
        )
        self.assertEqual(
            metadata["gallery_urls"],
            "https://saddlesisters.ca/sshp/saddle-sisters-team-photo.webp",
        )

    def test_extract_metadata_with_asset_fallback_reads_css_hero_from_normal_homepage(self) -> None:
        html = """
        <html>
          <head>
            <title>Women's Cycling Network</title>
            <link rel="stylesheet" href="/css/index.css">
          </head>
          <body>
            <img src="/asset/image/site-logo.png" alt="Women's Cycling Network logo">
          </body>
        </html>
        """
        resources = {
            "https://www.womenscyclingnetwork.ca/css/index.css": """
                .hero {
                    background: url('../asset/image/newCover1.jpg');
                }
            """,
        }

        def fetcher(url: str, timeout: float, user_agent: str) -> str:
            del timeout, user_agent
            return resources[url]

        metadata = extract_metadata_with_asset_fallback(
            html,
            "https://www.womenscyclingnetwork.ca/",
            timeout=10.0,
            user_agent="CFW-Test/1.0",
            fetcher=fetcher,
        )
        self.assertEqual(
            metadata["cover_url"],
            "https://www.womenscyclingnetwork.ca/asset/image/newCover1.jpg",
        )

    def test_extract_metadata_with_asset_fallback_reads_manifest_chunk_images(self) -> None:
        html = """
        <html>
          <head>
            <title>Saddle Sisters of High Park</title>
            <link rel="stylesheet" href="/static/css/main.css">
            <script defer src="/static/js/main.js"></script>
          </head>
          <body>
            <noscript>You need to enable JavaScript to run this app.</noscript>
            <div id="root"></div>
          </body>
        </html>
        """
        resources = {
            "https://saddlesisters.ca/asset-manifest.json": """
                {
                  "files": {
                    "main.css": "/static/css/main.css",
                    "main.js": "/static/js/main.js",
                    "chunk.js": "/static/js/chunk.js"
                  }
                }
            """,
            "https://saddlesisters.ca/static/css/main.css": "",
            "https://saddlesisters.ca/static/js/main.js": """
                const logo = "/static/media/saddle-sisters-logo.svg";
                const home = "sshp/homepage.jpg";
            """,
            "https://saddlesisters.ca/static/js/chunk.js": """
                const ridesHero = "sshp/grouprides0.jpg";
            """,
        }

        def fetcher(url: str, timeout: float, user_agent: str) -> str:
            del timeout, user_agent
            return resources[url]

        metadata = extract_metadata_with_asset_fallback(
            html,
            "https://saddlesisters.ca/",
            timeout=10.0,
            user_agent="CFW-Test/1.0",
            fetcher=fetcher,
        )
        self.assertEqual(
            metadata["logo_url"],
            "https://saddlesisters.ca/static/media/saddle-sisters-logo.svg",
        )
        self.assertEqual(
            metadata["cover_url"],
            "https://saddlesisters.ca/sshp/grouprides0.jpg",
        )
        self.assertEqual(
            metadata["gallery_urls"],
            "https://saddlesisters.ca/sshp/homepage.jpg",
        )

    def test_choose_asset_images_prefers_homepage_over_sponsor_graphic(self) -> None:
        logo, cover, gallery = choose_asset_images(
            [
                "https://saddlesisters.ca/sshp/homepage.jpg",
                "https://saddlesisters.ca/sshp/SSHPlogo-min.png",
                "https://saddlesisters.ca/sponsors/roadkit.jpg",
            ]
        )
        self.assertEqual(logo, "https://saddlesisters.ca/sshp/SSHPlogo-min.png")
        self.assertEqual(cover, "https://saddlesisters.ca/sshp/homepage.jpg")
        self.assertEqual(gallery, [])

    def test_apply_social_logo_fallback_prefers_instagram_profile_image(self) -> None:
        metadata = {
            "logo_url": "",
            "logo_rights_status": "",
            "instagram_url": "https://www.instagram.com/spokesisters/",
            "strava_url": "https://www.strava.com/clubs/738936",
            "facebook_url": "",
            "twitter_url": "",
            "tiktok_url": "",
            "youtube_url": "",
        }
        responses = {
            "https://www.instagram.com/spokesisters/": """
                <html>
                  <head>
                    <meta property="og:image" content="https://cdn.example.com/spokesisters-profile.jpg">
                  </head>
                </html>
            """
        }

        def fetcher(url: str, timeout: float, user_agent: str) -> str:
            del timeout, user_agent
            return responses[url]

        updated = apply_social_logo_fallback(
            metadata,
            timeout=10.0,
            user_agent="CFW-Test/1.0",
            fetcher=fetcher,
        )
        self.assertEqual(updated["logo_url"], "https://cdn.example.com/spokesisters-profile.jpg")
        self.assertEqual(updated["logo_rights_status"], "official-social")

    def test_sanitize_image_selection_keeps_logo_out_of_gallery(self) -> None:
        logo, cover, gallery = sanitize_image_selection(
            "https://example.com/assets/logo.svg",
            "https://example.com/assets/logo.svg",
            [
                "https://example.com/assets/logo.svg",
                "https://example.com/images/group-ride.jpg",
                "https://example.com/images/women-clinic.jpg",
                "https://example.com/images/favicon.png",
            ],
        )
        self.assertEqual(logo, "https://example.com/assets/logo.svg")
        self.assertEqual(cover, "https://example.com/images/group-ride.jpg")
        self.assertEqual(gallery, ["https://example.com/images/women-clinic.jpg"])

    def test_sanitize_image_selection_prefers_landscape_with_dimension_hints(self) -> None:
        logo, cover, gallery = sanitize_image_selection(
            "",
            "https://example.com/images/group-portrait.jpg",
            ["https://example.com/images/group-landscape.jpg"],
            dimension_hints={
                "https://example.com/images/group-portrait.jpg": (700, 1100),
                "https://example.com/images/group-landscape.jpg": (1400, 800),
            },
        )
        self.assertEqual(logo, "")
        self.assertEqual(cover, "https://example.com/images/group-landscape.jpg")
        self.assertEqual(gallery, ["https://example.com/images/group-portrait.jpg"])

    def test_homepage_candidate_urls_prioritize_aspdotnet_homepage(self) -> None:
        self.assertEqual(
            homepage_candidate_urls("https://www.wildbettys.com/ClubInfo/AboutWildBettys.aspx")[:3],
            [
                "https://www.wildbettys.com/Home.aspx",
                "https://www.wildbettys.com/Default.aspx",
                "https://www.wildbettys.com/",
            ],
        )

    def test_inference_helpers(self) -> None:
        self.assertEqual(infer_audience("A women only mountain bike club"), "Women Only")
        self.assertEqual(
            infer_disciplines("Road and gravel rides with cyclocross season"),
            ("Road", "Road,Gravel,Cyclo-cross"),
        )

    def test_build_staging_row_uses_seed_fallbacks(self) -> None:
        seed = LocationSeed(
            location_id="toronto-on",
            location_name="Toronto, Ontario, Canada",
            city="Toronto",
            region="Ontario",
            country="Canada",
            postal_code="",
            latitude="43.6532",
            longitude="-79.3832",
            query_hint="road gravel mtb",
        )
        result = SearchResult(
            query='women cycling club "Toronto" "Ontario"',
            title="Wild Bettys",
            link="https://wildbettys.com/",
            snippet="A Toronto-based club for women mountain bikers.",
        )
        metadata = {
            "title": "Wild Bettys",
            "description": "A Toronto-based club for women mountain bikers.",
            "canonical_url": "https://wildbettys.com/",
            "site_url": "https://wildbettys.com/",
            "site_name": "Wild Bettys",
            "logo_url": "https://wildbettys.com/images/logo.png",
            "cover_url": "https://wildbettys.com/images/hero.jpg",
            "gallery_urls": "https://wildbettys.com/images/hero.jpg::https://wildbettys.com/images/team-ride.jpg",
            "facebook_url": "",
            "instagram_url": "",
            "twitter_url": "",
            "youtube_url": "",
            "strava_url": "",
            "email": "",
            "street": "",
            "city": "",
            "region": "",
            "country": "",
            "postal_code": "",
            "latitude": "",
            "longitude": "",
        }
        row = build_staging_row(seed=seed, result=result, metadata=metadata, score=0.82, discovered_at=__import__("datetime").datetime(2026, 4, 14, 17, 0, 0))
        self.assertEqual(row["city"], "Toronto")
        self.assertEqual(row["region"], "Ontario")
        self.assertEqual(row["country"], "Canada")
        self.assertEqual(row["audience"], "Women Only")
        self.assertEqual(row["discipline_primary"], "MTB")
        self.assertEqual(row["disciplines_csv"], "MTB")
        self.assertEqual(row["logo_rights_status"], "official-site")
        self.assertEqual(
            row["gallery_source_urls"],
            "https://wildbettys.com/images/team-ride.jpg",
        )
        self.assertIn("location uses seed city", row["notes_internal"])

    def test_score_result_prefers_relevant_candidates(self) -> None:
        seed = LocationSeed(
            location_id="ottawa-on",
            location_name="Ottawa, Ontario, Canada",
            city="Ottawa",
            region="Ontario",
            country="Canada",
            postal_code="",
            latitude="45.4215",
            longitude="-75.6972",
            query_hint="",
        )
        good = SearchResult(
            query="",
            title="Ottawa Bicycle Club women's road rides",
            link="https://ottawabicycleclub.ca/",
            snippet="A cycling club in Ottawa with road, gravel and women's rides.",
        )
        bad = SearchResult(
            query="",
            title="Bike Shop Ottawa",
            link="https://example.com/bike-shop-ottawa",
            snippet="Repairs, rentals and accessories in Ottawa.",
        )
        self.assertGreater(score_result(seed, good), score_result(seed, bad))

    def test_choose_club_name_skips_generic_metadata_titles(self) -> None:
        result = SearchResult(
            query="",
            title="Ottawa Bicycle Club",
            link="https://ottawabicycleclub.ca/",
            snippet="Cycling club in Ottawa.",
        )
        metadata = {
            "site_name": "Home",
            "title": "Home",
        }
        self.assertEqual(choose_club_name(result, metadata), "Ottawa Bicycle Club")

    def test_build_queries_includes_hint(self) -> None:
        seed = LocationSeed(
            location_id="kw-on",
            location_name="Waterloo Region",
            city="Waterloo",
            region="Ontario",
            country="Canada",
            postal_code="",
            latitude="",
            longitude="",
            query_hint="gravel mtb",
        )
        queries = build_queries(seed, ['women cycling club "{city}" "{region}"'])
        self.assertEqual(queries, ['women cycling club "Waterloo" "Ontario" gravel mtb'])

    def test_place_helpers(self) -> None:
        seed = LocationSeed(
            location_id="sf-ca",
            location_name="San Francisco, California, United States",
            city="San Francisco",
            region="California",
            country="United States",
            postal_code="",
            latitude="37.7749",
            longitude="-122.4194",
            query_hint="road gravel",
        )
        self.assertEqual(place_location_string(seed), "San Francisco California United States")
        self.assertEqual(
            build_place_queries(seed, ["cycling club"]),
            ["cycling club road gravel"],
        )

        place_result = {
            "title": "Wild Bettys",
            "url": "https://wildbettys.com/",
            "provider_url": "https://maps.example.com/wild-bettys",
            "description": "Women's mountain bike club",
            "coordinates": {"latitude": 43.7400452, "longitude": -79.328443},
            "postal_address": {
                "streetAddress": "1191 Lawrence Avenue East",
                "addressLocality": "Toronto",
                "addressRegion": "Ontario",
                "postalCode": "M3A 3R3",
                "country": "CA",
            },
            "contact": {"email": "hello@wildbettys.com"},
            "profiles": [
                {"url": "https://www.instagram.com/wildbettys/"},
                {"url": "https://www.facebook.com/WildBettys/"},
            ],
            "thumbnail": {"original": "https://maps.example.com/thumb.jpg"},
            "categories": [{"name": "Sports Club"}],
        }
        metadata = metadata_from_place_result(place_result)
        self.assertEqual(metadata["site_url"], "https://wildbettys.com/")
        self.assertEqual(metadata["country"], "Canada")
        self.assertEqual(metadata["instagram_url"], "https://www.instagram.com/wildbettys")
        self.assertEqual(metadata["image_rights_status"], "unknown")


if __name__ == "__main__":
    unittest.main()
