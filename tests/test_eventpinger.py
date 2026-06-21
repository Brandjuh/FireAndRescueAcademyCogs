import asyncio
import types

from eventpinger.eventpinger import (
    MISSIONCHIEF_APP_ID,
    NOTIFY_EVENT_ROLE_ID,
    SOURCE_CHANNEL_ID,
    extract_announcement_from_message,
    find_region_role,
    resolve_region,
    state_from_zip,
    EventPinger,
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


def test_uncertain_address_returns_none():
    assert resolve_region("Main Street near the park") is None


def test_finds_region_role_by_hardcoded_name():
    role = FakeRole(1, "New York (NY)")
    guild = FakeGuild([role])

    assert find_region_role(guild, "NY") is role


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
