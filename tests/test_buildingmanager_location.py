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
            detected_facility_type="hospital",
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
            "This location does not clearly look like a real hospital or prison/jail.",
        )

    def test_detect_supported_building_type_accepts_real_hospital(self):
        details = LocationDetails(
            original_input="input",
            resolved_input="https://www.google.com/maps/place/Example+Hospital",
            place_name="Example Regional Hospital",
            detected_facility_type="hospital health",
        )

        building_type, reason = LocationParser.detect_supported_building_type(details)

        self.assertEqual(building_type, "Hospital")
        self.assertIn("hospital", reason)

    def test_detect_supported_building_type_rejects_clinic(self):
        details = LocationDetails(
            original_input="input",
            resolved_input="https://www.google.com/maps/place/Example+Clinic",
            place_name="Example Medical Clinic",
            detected_facility_type="clinic health",
        )

        building_type, reason = LocationParser.detect_supported_building_type(details)

        self.assertIsNone(building_type)
        self.assertIn("clinic", reason)

    def test_detect_supported_building_type_accepts_prison(self):
        details = LocationDetails(
            original_input="input",
            resolved_input="https://www.google.com/maps/place/Example+Correctional+Facility",
            place_name="Example Correctional Facility",
            detected_facility_type="amenity prison",
        )

        building_type, reason = LocationParser.detect_supported_building_type(details)

        self.assertEqual(building_type, "Prison")
        self.assertIn("prison", reason)

    def test_detect_supported_building_type_rejects_historic_jail_museum(self):
        details = LocationDetails(
            original_input="input",
            resolved_input="https://www.google.com/maps/place/Old+Jail+Museum",
            place_name="Old Jail Museum",
            detected_facility_type="tourism museum",
        )

        building_type, reason = LocationParser.detect_supported_building_type(details)

        self.assertIsNone(building_type)
        self.assertIn("museum", reason)

    def test_detect_supported_building_type_rejects_courthouse(self):
        details = LocationDetails(
            original_input="input",
            resolved_input="https://www.google.com/maps/place/County+Courthouse",
            place_name="County Courthouse",
            detected_facility_type="amenity courthouse",
        )

        building_type, reason = LocationParser.detect_supported_building_type(details)

        self.assertIsNone(building_type)
        self.assertIn("courthouse", reason)

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

    def test_resolve_location_uses_place_name_when_link_has_no_coordinates(self):
        original_forward = LocationParser.forward_geocode_nominatim_details
        original_expand = LocationParser.expand_maps_url

        async def fake_expand(text):
            return text

        async def fake_forward(query):
            self.assertEqual(query, "Máxima MC Eindhoven")
            return {
                "coordinates": "51.4541382, 5.4871691",
                "address": "Máxima MC, Eindhoven, Netherlands",
                "place_name": "Máxima MC Eindhoven",
                "country": "Netherlands",
                "region": "North Brabant",
                "provider": "nominatim",
                "facility_type": "hospital",
            }

        try:
            LocationParser.expand_maps_url = fake_expand
            LocationParser.forward_geocode_nominatim_details = fake_forward
            details = asyncio.run(
                LocationParser.resolve_location("https://www.google.nl/maps/place/M%C3%A1xima+MC+Eindhoven/")
            )
        finally:
            LocationParser.expand_maps_url = original_expand
            LocationParser.forward_geocode_nominatim_details = original_forward

        self.assertEqual(details.coordinates, "51.4541382, 5.4871691")
        self.assertEqual(details.place_name, "Máxima MC Eindhoven")
        self.assertEqual(details.country, "Netherlands")
        self.assertIn("51.4541382", details.maps_url)


if __name__ == "__main__":
    unittest.main()
