import asyncio
import types

from missionsdatabase.database import MissionsDatabase as MissionStore
from missionsdatabase.mission_fetcher import MissionFetcher
from missionsdatabase.mission_formatter import MissionFormatter
from missionsdatabase.missionsdatabase import MissionsDatabase


MISSION_PAYLOAD = {
    "0": {
        "name": "Bin fire",
        "average_credits": 100,
        "requirements": {"firetrucks": 1},
        "additional": {},
        "prerequisites": {"main_building": 0, "fire_stations": 1},
        "mission_categories": ["fire", "urban"],
    },
    "2": {
        "id": "2",
        "name": "Burning car",
        "average_credits": 600,
        "requirements": {"firetrucks": 1, "police_cars": 1},
        "additional": {"possible_patient": 1},
        "prerequisites": {"main_building": 0, "fire_stations": 2},
        "mission_categories": ["fire", "tow_trucks", "urban"],
    },
    "2a": {
        "id": "2",
        "base_mission_id": 2,
        "additive_overlays": "a",
        "name": "Burning car",
        "average_credits": 800,
        "requirements": {"firetrucks": 2},
        "additional": {},
        "prerequisites": {"main_building": 0, "fire_stations": 3},
        "mission_categories": ["fire", "tow_trucks", "urban"],
    },
}


class FakeMessage:
    def __init__(self, message_id, *, content=None, embed=None):
        self.id = message_id
        self.content = content
        self.embeds = [embed] if embed else []
        self.edit_count = 0

    async def edit(self, **kwargs):
        self.edit_count += 1
        if "content" in kwargs:
            self.content = kwargs["content"]
        if "embed" in kwargs:
            self.embeds = [kwargs["embed"]]


class FakeTextChannel:
    id = 1518038840152031262
    mention = "#possible-missions"

    def __init__(self):
        self.messages = []
        self.next_id = 100

    async def send(self, **kwargs):
        message = FakeMessage(self.next_id, content=kwargs.get("content"), embed=kwargs.get("embed"))
        self.next_id += 1
        self.messages.append(message)
        return message

    async def fetch_message(self, message_id):
        for message in self.messages:
            if message.id == message_id:
                return message
        raise RuntimeError("message not found")

    def history(self, *, limit):
        _ = limit

        async def iterator():
            for message in reversed(self.messages):
                yield message

        return iterator()


class FakeGuild:
    id = 123

    def __init__(self, channel):
        self.channel = channel

    def get_channel(self, channel_id):
        if channel_id == self.channel.id:
            return self.channel
        return None


class FakeFetcher:
    async def fetch_missions(self):
        return MissionFetcher.normalize_missions(MISSION_PAYLOAD)

    async def close(self):
        return None


def test_mission_fetcher_normalizes_dict_payload_and_overlay_keys():
    missions = MissionFetcher.normalize_missions(MISSION_PAYLOAD)
    keys = [MissionFetcher.mission_key(mission) for mission in missions]

    assert keys == ["0", "2", "2/a"]
    assert MissionFetcher.detail_url(missions[2]).endswith("/einsaetze/2?additive_overlays=a")


def test_mission_formatter_builds_marker_content_and_embed_footer():
    mission = MissionFetcher.normalize_missions(MISSION_PAYLOAD)[1]

    content = MissionFormatter.build_content(mission)
    embed = MissionFormatter.build_embed(mission)

    assert content == "MissionChief Possible Mission: `2`"
    assert embed.kwargs["title"] == "2 - Burning car"
    assert any(field["name"] == "Vehicle / Equipment Requirements" for field in embed.fields)
    assert embed.footer["text"] == "Mission ID: 2 | Source: MissionChief Possible Missions"


def test_safe_sync_creates_then_skips_existing_messages(tmp_path):
    async def run():
        channel = FakeTextChannel()
        guild = FakeGuild(channel)
        bot = types.SimpleNamespace(guilds=[guild])
        cog = MissionsDatabase(bot)
        cog.fetcher = FakeFetcher()
        cog.db = MissionStore(tmp_path / "missions.db")
        await cog.db.initialize()
        await cog.db.set_config(guild.id, channel.id)

        first = await cog._sync_missions(
            guild,
            limit=2,
            query=None,
            force_update=False,
        )
        second = await cog._sync_missions(
            guild,
            limit=2,
            query=None,
            force_update=False,
        )

        assert first["created"] == 2
        assert first["selected_missions"] == 2
        assert second["created"] == 0
        assert second["skipped"] == 2
        assert len(channel.messages) == 2

    asyncio.run(run())


def test_missing_db_record_recovers_existing_message(tmp_path):
    async def run():
        channel = FakeTextChannel()
        guild = FakeGuild(channel)
        bot = types.SimpleNamespace(guilds=[guild])
        cog = MissionsDatabase(bot)
        cog.fetcher = FakeFetcher()
        cog.db = MissionStore(tmp_path / "missions.db")
        await cog.db.initialize()
        await cog.db.set_config(guild.id, channel.id)

        mission = MissionFetcher.normalize_missions(MISSION_PAYLOAD)[0]
        await channel.send(
            content=MissionFormatter.build_content(mission),
            embed=MissionFormatter.build_embed(mission),
        )

        stats = await cog._sync_missions(
            guild,
            limit=1,
            query=None,
            force_update=True,
        )

        assert stats["recovered"] == 1
        assert stats["created"] == 0
        assert len(channel.messages) == 1

    asyncio.run(run())
