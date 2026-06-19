import asyncio
import unittest

from buildingmanager.buildingmanager import LocationDetails, LocationParser


class BuildingManagerLocationTests(unittest.TestCase):
    def test_google_place_url_uses_exact_place_marker_coordinates(self):
        url = (
            "https://www.google.nl/maps/place/M%C3%A1xima+MC+Eindhoven/"
            "@51.4545623,5.451513,7561m/data=!3m1!1e3!4m6!3m5!"
            "1s0x47c6d8df50052be7:0xf301955a8913ea08!"
            "8m2!3d51.4541382!4d5.4871691!16s%2Fm%2F011pz4z2"
        )

        self.assertEqual(
            LocationParser.extract_coordinates(url),
            (51.4541382, 5.4871691),
        )

    def test_google_place_name_is_decoded_for_facility_check(self):
        url = "https://www.google.nl/maps/place/M%C3%A1xima+MC+Eindhoven/@51,5"

        self.assertEqual(
            LocationParser.extract_place_name(url),
            "Máxima MC Eindhoven",
        )

    def test_hospital_facility_check_accepts_healthcare_place_name(self):
        details = LocationDetails(
            original_input="input",
            resolved_input="https://www.google.nl/maps/place/M%C3%A1xima+MC+Eindhoven/@51,5",
        )

        self.assertIsNone(
            LocationParser.facility_warning("Hospital", "Máxima MC Eindhoven", details)
        )

    def test_prison_facility_check_warns_for_unrelated_location(self):
        details = LocationDetails(
            original_input="input",
            resolved_input="https://www.google.com/maps/place/Random+Park/@51,5",
            address="Random Park, Eindhoven, Netherlands",
        )

        self.assertEqual(
            LocationParser.facility_warning("Prison", "Random Park", details),
            "This location does not clearly look like a justice or correctional facility.",
        )

    def test_google_result_details_include_country_region_and_coordinates(self):
        result = {
            "formatted_address": "Máxima MC, Eindhoven, Netherlands",
            "geometry": {"location": {"lat": 51.4541382, "lng": 5.4871691}},
            "types": ["hospital", "health"],
            "address_components": [
                {"long_name": "North Brabant", "types": ["administrative_area_level_1"]},
                {"long_name": "Netherlands", "types": ["country"]},
            ],
        }

        details = LocationParser._google_result_to_details(result)

        self.assertEqual(details["coordinates"], "51.4541382, 5.4871691")
        self.assertEqual(details["country"], "Netherlands")
        self.assertEqual(details["region"], "North Brabant")
        self.assertEqual(details["facility_type"], "hospital health")

    def test_resolve_location_uses_forward_geocode_for_plus_code(self):
        original_forward = LocationParser.forward_geocode_details
        original_expand = LocationParser.expand_maps_url

        async def fake_expand(text):
            return text

        async def fake_forward(query, *, google_key=None):
            self.assertEqual(query, "FF3P+MV Eindhoven")
            self.assertEqual(google_key, "key")
            return {
                "coordinates": "51.4541382, 5.4871691",
                "address": "Eindhoven, Netherlands",
                "country": "Netherlands",
                "region": "North Brabant",
                "provider": "google",
                "facility_type": "plus_code",
            }

        try:
            LocationParser.expand_maps_url = fake_expand
            LocationParser.forward_geocode_details = fake_forward
            details = asyncio.run(
                LocationParser.resolve_location("FF3P+MV Eindhoven", google_key="key")
            )
        finally:
            LocationParser.expand_maps_url = original_expand
            LocationParser.forward_geocode_details = original_forward

        self.assertEqual(details.coordinates, "51.4541382, 5.4871691")
        self.assertEqual(details.country, "Netherlands")
        self.assertIn("51.4541382", details.maps_url)


if __name__ == "__main__":
    unittest.main()
