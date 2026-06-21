import asyncio
import types

from eventpinger.eventpinger import (
    EventPinger,
    MISSIONCHIEF_APP_ID,
    NOTIFY_EVENT_ROLE_ID,
    RegionMatch,
    SOURCE_CHANNEL_ID,
    extract_announcement_from_message,
    find_region_role,
    region_from_geocode_results,
    resolve_region,
    state_from_zip,
)


class FakeRole:
    def __init__(self, role_id, name):
        self.id = role_id
        self.name = name
        self.mention = f"<@&{role_id}>"


class FakeGuild:
    def __init__(self, roles):
        self.roles = roles

    def get_role(self, role_id):
        for role in self.roles:
            if role.id == role_id:
                return role
        return None


class FakeChannel:
    id = SOURCE_CHANNEL_ID

    def __init__(self):
        self.sent = []

    async def send(self, content, **kwargs):
        self.sent.append((content, kwargs))


def fake_message(*, title, description, author_id=MISSIONCHIEF_APP_ID, channel=None, guild=None):
    embed = types.SimpleNamespace(title=title, description=description, fields=[])
    return types.SimpleNamespace(
        author=types.SimpleNamespace(id=author_id),
        channel=channel or types.SimpleNamespace(id=SOURCE_CHANNEL_ID),
        guild=guild,
        embeds=[embed],
        content="",
    )


def test_extracts_alliance_mission_embed():
    message = fake_message(
        title="Start alliance mission! Test Mission",
        description="71 East 153rd Street, 10451 New York, The Bronx",
    )

    announcement = extract_announcement_from_message(message)

    assert announcement.kind == "mission"
    assert announcement.name == "Test Mission"
    assert announcement.address == "71 East 153rd Street, 10451 New York, The Bronx"


def test_resolves_us_zip_to_new_york():
    match = resolve_region("71 East 153rd Street, 10451 New York, The Bronx")

    assert match.code == "NY"
    assert match.name == "New York (NY)"
    assert match.source == "us_zip"


def test_zip_prefix_201_resolves_to_virginia_not_dc():
    assert state_from_zip("20101") == "VA"


def test_resolves_bermuda_postal_code_instead_of_florida():
    match = resolve_region("FL 04 Flatts")

    assert match.code == "BM"
    assert match.name == "Bermuda (BM)"


def test_geocode_result_resolves_bermuda_country():
    match = region_from_geocode_results(
        [
            {
                "display_name": "Flatts, Bermuda",
                "address": {
                    "country": "Bermuda",
                    "country_code": "bm",
                },
            }
        ]
    )

    assert match.code == "BM"
    assert match.source == "geocode_country"


def test_geocode_result_resolves_us_state():
    match = region_from_geocode_results(
        [
            {
                "display_name": "Los Angeles, California, United States",
                "address": {
                    "city": "Los Angeles",
                    "state": "California",
                    "country": "United States",
                    "country_code": "us",
                },
            }
        ]
    )

    assert match.code == "CA"
    assert match.source == "geocode_state"


def test_geocode_result_resolves_global_country():
    match = region_from_geocode_results(
        [
            {
                "display_name": "Oberhausen, North Rhine-Westphalia, Germany",
                "address": {
                    "city": "Oberhausen",
                    "country": "Germany",
                    "country_code": "de",
                },
            }
        ]
    )

    assert match.code == "COUNTRY:DE"
    assert match.name == "Germany (DE)"
    assert match.source == "geocode_country"
    assert "Germany (DE)" in match.role_names


def test_uncertain_address_returns_none():
    assert resolve_region("Main Street near the park") is None


def test_european_postal_code_does_not_resolve_as_us_zip_without_us_context():
    assert resolve_region("52 Bogenstra\u00dfe, 46045 Oberhausen, Altstaden") is None


def test_finds_region_role_by_hardcoded_name():
    role = FakeRole(1, "New York (NY)")
    guild = FakeGuild([role])

    assert find_region_role(guild, "NY") is role


def test_finds_global_country_role_by_exact_name_without_state_code_confusion():
    california = FakeRole(1, "California (CA)")
    canada = FakeRole(2, "Canada (CA)")
    guild = FakeGuild([california, canada])
    match = RegionMatch("COUNTRY:CA", "Canada (CA)", "geocode_country", ("Canada (CA)", "Canada"))

    assert find_region_role(guild, match) is canada


def test_on_message_pings_notify_and_region_role():
    async def run():
        notify = FakeRole(NOTIFY_EVENT_ROLE_ID, "Notify-Event")
        state = FakeRole(2, "New York (NY)")
        guild = FakeGuild([notify, state])
        channel = FakeChannel()
        message = fake_message(
            title="Start alliance mission! Test Mission",
            description="71 East 153rd Street, 10451 New York, The Bronx",
            channel=channel,
            guild=guild,
        )
        cog = EventPinger(types.SimpleNamespace())

        await cog.on_message(message)

        content, _ = channel.sent[0]
        assert notify.mention in content
        assert state.mention in content
        assert "Region: New York (NY)" in content

    asyncio.run(run())


def test_on_message_unresolved_address_pings_notify_only():
    async def run():
        notify = FakeRole(NOTIFY_EVENT_ROLE_ID, "Notify-Event")
        guild = FakeGuild([notify, FakeRole(2, "Florida (FL)")])
        channel = FakeChannel()
        message = fake_message(
            title="Alliance event started! Storm Surge",
            description="Unknown shoreline",
            channel=channel,
            guild=guild,
        )
        cog = EventPinger(types.SimpleNamespace())

        await cog.on_message(message)

        content, _ = channel.sent[0]
        assert notify.mention in content
        assert "<@&2>" not in content
        assert "Unresolved" in content

    asyncio.run(run())


def test_async_geocode_resolver_uses_api_before_local_fallback():
    async def run():
        calls = []
        cog = EventPinger(types.SimpleNamespace())

        async def get_key():
            return "test-key"

        async def enabled():
            return True

        async def fetch(address, api_key):
            calls.append((address, api_key))
            return [
                {
                    "address": {
                        "state": "California",
                        "country": "United States",
                        "country_code": "us",
                    }
                }
            ]

        cog._get_geocode_api_key = get_key
        cog._geocode_enabled = enabled
        cog._fetch_geocode_results = fetch

        first = await cog.resolve_region_for_address("FL 04 Flatts")
        second = await cog.resolve_region_for_address("FL 04 Flatts")

        assert first.code == "CA"
        assert first.source == "geocode_state"
        assert second.code == "CA"
        assert second.source == "geocode_state_cache"
        assert calls == [("FL 04 Flatts", "test-key")]

    asyncio.run(run())


def test_async_geocode_failure_falls_back_to_local_resolver():
    async def run():
        cog = EventPinger(types.SimpleNamespace())

        async def get_key():
            return "test-key"

        async def enabled():
            return True

        async def fetch(address, api_key):
            raise RuntimeError("api unavailable")

        cog._get_geocode_api_key = get_key
        cog._geocode_enabled = enabled
        cog._fetch_geocode_results = fetch

        match = await cog.resolve_region_for_address("71 East 153rd Street, 10451 New York, The Bronx")

        assert match.code == "NY"
        assert match.source == "us_zip"

    asyncio.run(run())


def test_async_geocode_country_prevents_us_zip_fallback_for_european_address():
    async def run():
        cog = EventPinger(types.SimpleNamespace())

        async def get_key():
            return "test-key"

        async def enabled():
            return True

        async def fetch(address, api_key):
            return [
                {
                    "address": {
                        "city": "Oberhausen",
                        "country": "Germany",
                        "country_code": "de",
                    }
                }
            ]

        cog._get_geocode_api_key = get_key
        cog._geocode_enabled = enabled
        cog._fetch_geocode_results = fetch

        match = await cog.resolve_region_for_address("52 Bogenstra\u00dfe, 46045 Oberhausen, Altstaden")

        assert match.code == "COUNTRY:DE"
        assert match.name == "Germany (DE)"
        assert match.source == "geocode_country"

    asyncio.run(run())
