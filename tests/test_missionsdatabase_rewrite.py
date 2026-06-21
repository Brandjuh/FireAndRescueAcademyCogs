import asyncio
import types

import discord

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
    "438": {
        "id": "438",
        "base_mission_id": 438,
        "name": "Serve court order",
        "average_credits": 150,
        "requirements": {"police_cars": 1},
        "additional": {},
        "prerequisites": {"main_building": 6, "police_stations": 3},
        "mission_categories": ["police"],
    },
    "438-0": {
        "id": "438-0",
        "base_mission_id": 438,
        "additive_overlays": "",
        "name": "Serve court order",
        "average_credits": 550,
        "requirements": {"police_cars": 2},
        "additional": {},
        "prerequisites": {"main_building": 6, "police_stations": 7},
        "mission_categories": ["police"],
    },
    "1152": {
        "id": "1152",
        "base_mission_id": 1152,
        "name": "Late numeric mission",
        "average_credits": 1000,
        "requirements": {"firetrucks": 1},
        "additional": {},
        "prerequisites": {"main_building": 0, "fire_stations": 1},
        "mission_categories": ["fire"],
    },
}


class FakeMessage:
    def __init__(self, message_id, *, content=None, embed=None, channel=None):
        self.id = message_id
        self.content = content
        self.embeds = [embed] if embed else []
        self.channel = channel
        self.deleted = False
        self.edit_count = 0

    async def edit(self, **kwargs):
        self.edit_count += 1
        if "content" in kwargs:
            self.content = kwargs["content"]
        if "embed" in kwargs:
            self.embeds = [kwargs["embed"]]

    async def delete(self):
        self.deleted = True
        if self.channel and self in self.channel.messages:
            self.channel.messages.remove(self)


class FakeTextChannel:
    id = 1518038840152031262
    mention = "#possible-missions"

    def __init__(self):
        self.messages = []
        self.next_id = 100

    async def send(self, **kwargs):
        message = FakeMessage(
            self.next_id,
            content=kwargs.get("content"),
            embed=kwargs.get("embed"),
            channel=self,
        )
        self.next_id += 1
        self.messages.append(message)
        return message

    async def fetch_message(self, message_id):
        for message in self.messages:
            if message.id == message_id:
                return message
        raise discord.NotFound()

    def history(self, *, limit):
        _ = limit

        async def iterator():
            for message in list(reversed(self.messages)):
                yield message

        return iterator()


class FakeForumThread:
    def __init__(self, thread_id, *, name, content=None, embed=None):
        self.id = thread_id
        self.name = name
        self.archived = False
        self.deleted = False
        self.applied_tags = []
        self.message = FakeMessage(thread_id, content=content, embed=embed)

    async def fetch_message(self, message_id):
        if message_id == self.message.id:
            return self.message
        raise discord.NotFound()

    async def edit(self, **kwargs):
        if "name" in kwargs:
            self.name = kwargs["name"]
        if "archived" in kwargs:
            self.archived = kwargs["archived"]
        if "applied_tags" in kwargs:
            self.applied_tags = kwargs["applied_tags"]

    async def delete(self):
        self.deleted = True


class FakeForumChannel:
    id = 1518038840152031262
    mention = "#possible-missions"
    available_tags = []

    def __init__(self, *, active_limit=1000):
        self.threads = []
        self.next_id = 500
        self.active_limit = active_limit
        self.created_kwargs = []

    def add_existing_thread(self, *, name, content, embed):
        thread = FakeForumThread(self.next_id, name=name, content=content, embed=embed)
        self.next_id += 1
        self.threads.append(thread)
        return thread

    async def create_thread(self, **kwargs):
        self.created_kwargs.append(kwargs)
        active_threads = sum(1 for thread in self.threads if not thread.archived)
        if active_threads >= self.active_limit:
            raise discord.HTTPException()

        thread = self.add_existing_thread(
            name=kwargs["name"],
            content=kwargs.get("content"),
            embed=kwargs.get("embed"),
        )
        return types.SimpleNamespace(thread=thread, message=thread.message)

    def get_thread(self, thread_id):
        for thread in self.threads:
            if thread.id == thread_id:
                return thread
        return None

    def archived_threads(self, *, limit):
        async def iterator():
            yielded = 0
            for thread in list(self.threads):
                if not thread.archived:
                    continue
                if limit is not None and yielded >= limit:
                    return
                yielded += 1
                yield thread

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

    assert keys == ["0", "2", "2/a", "438", "438-0", "1152"]
    assert MissionFetcher.detail_url(missions[2]).endswith("/einsaetze/2?additive_overlays=a")


def test_mission_fetcher_sorts_hyphen_variants_by_base_id_and_uses_overlay_index_url():
    missions = MissionFetcher.normalize_missions(MISSION_PAYLOAD)
    keys = [MissionFetcher.mission_key(mission) for mission in missions]
    variant = next(mission for mission in missions if MissionFetcher.mission_key(mission) == "438-0")

    assert keys.index("438-0") == keys.index("438") + 1
    assert keys.index("438-0") < keys.index("1152")
    assert MissionFetcher.detail_url(variant).endswith("/einsaetze/438?overlay_index=0")


def test_mission_formatter_builds_marker_content_and_embed_footer():
    mission = MissionFetcher.normalize_missions(MISSION_PAYLOAD)[1]

    content = MissionFormatter.build_content(mission)
    embed = MissionFormatter.build_embed(mission)

    assert content == "MissionChief Possible Mission: `2`"
    assert embed.kwargs["title"] == "2 - Burning car"
    assert any(field["name"] == "Vehicle / Equipment Requirements" for field in embed.fields)
    assert embed.footer["text"] == "Mission ID: 2 | Source: MissionChief Possible Missions"


def test_mission_formatter_includes_other_information_for_tow_overlay():
    payload = {
        "2a": {
            "id": "2/a",
            "base_mission_id": 2,
            "additive_overlays": "a",
            "name": "Burning car",
            "average_credits": 670,
            "requirements": {"firetrucks": 1},
            "additional": {
                "expansion_missions_ids": [26, 16, 19],
                "possible_crashed_car_min": 1,
                "possible_crashed_car_max": 1,
            },
            "prerequisites": {"main_building": 0, "fire_stations": 1, "tow_trucks": 1},
            "mission_categories": ["fire", "urban", "tow_trucks"],
        },
        "16": {"id": "16", "name": "Caravan fire"},
        "19": {"id": "19", "name": "Burning trailer"},
        "26": {"id": "26", "name": "Garage fire"},
    }
    mission = next(
        mission
        for mission in MissionFetcher.normalize_missions(payload)
        if MissionFetcher.mission_key(mission) == "2/a"
    )

    embed = MissionFormatter.build_embed(mission)
    other_info = next(field["value"] for field in embed.fields if field["name"] == "Other Information")

    assert "Expandable Missions: Garage fire, Caravan fire, Burning trailer" in other_info
    assert "Minimum cars to tow: 1" in other_info
    assert "Maximum cars to tow: 1" in other_info
    assert "Mission Variation: Burning car" in other_info


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


def test_deleted_recorded_message_is_recreated_instead_of_skipped(tmp_path):
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
            limit=1,
            query=None,
            force_update=False,
        )
        channel.messages.clear()

        second = await cog._sync_missions(
            guild,
            limit=1,
            query=None,
            force_update=False,
        )

        assert first["created"] == 1
        assert second["skipped"] == 0
        assert second["created"] == 1
        assert len(channel.messages) == 1

    asyncio.run(run())


def test_stop_request_stops_sync_after_current_message(tmp_path):
    async def run():
        channel = FakeTextChannel()
        guild = FakeGuild(channel)
        bot = types.SimpleNamespace(guilds=[guild])
        cog = MissionsDatabase(bot)
        cog.POST_DELAY_SECONDS = 0
        cog.BATCH_DELAY_SECONDS = 0
        cog.fetcher = FakeFetcher()
        cog.db = MissionStore(tmp_path / "missions.db")
        await cog.db.initialize()
        await cog.db.set_config(guild.id, channel.id)

        original_publish = cog._publish_mission

        async def publish_and_stop(*args, **kwargs):
            result = await original_publish(*args, **kwargs)
            cog._request_stop()
            return result

        cog._publish_mission = publish_and_stop
        stats = await cog._sync_missions(
            guild,
            limit=3,
            query=None,
            force_update=False,
        )

        assert stats["created"] == 1
        assert stats["stopped"] == 1
        assert len(channel.messages) == 1

    asyncio.run(run())


def test_wipe_configured_text_channel_deletes_tracked_posts_and_clears_db(tmp_path):
    async def run():
        channel = FakeTextChannel()
        guild = FakeGuild(channel)
        bot = types.SimpleNamespace(guilds=[guild])
        cog = MissionsDatabase(bot)
        cog.POST_DELAY_SECONDS = 0
        cog.BATCH_DELAY_SECONDS = 0
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
        stats = await cog._wipe_configured_posts(guild)
        db_stats = await cog.db.get_statistics(guild.id)

        assert first["created"] == 2
        assert stats["deleted"] == 2
        assert stats["failed"] == 0
        assert channel.messages == []
        assert db_stats["total"] == 0

    asyncio.run(run())


def test_forum_sync_archives_existing_threads_before_creating_new_posts(tmp_path):
    async def run():
        channel = FakeForumChannel(active_limit=2)
        guild = FakeGuild(channel)
        bot = types.SimpleNamespace(guilds=[guild])
        cog = MissionsDatabase(bot)
        cog.POST_DELAY_SECONDS = 0
        cog.BATCH_DELAY_SECONDS = 0
        cog.fetcher = FakeFetcher()
        cog.db = MissionStore(tmp_path / "missions.db")
        await cog.db.initialize()
        await cog.db.set_config(guild.id, channel.id)

        missions = MissionFetcher.normalize_missions(MISSION_PAYLOAD)
        for mission in missions[:2]:
            mission_key = MissionFetcher.mission_key(mission)
            thread = channel.add_existing_thread(
                name=MissionFormatter.thread_title(mission),
                content=MissionFormatter.build_content(mission),
                embed=MissionFormatter.build_embed(mission),
            )
            await cog.db.upsert_publication(
                guild_id=guild.id,
                mission_key=mission_key,
                channel_id=channel.id,
                target_kind="forum_thread",
                message_id=thread.message.id,
                thread_id=thread.id,
                content_hash=MissionFetcher.calculate_hash(
                    mission,
                    format_version=MissionFormatter.FORMAT_VERSION,
                ),
                title=thread.name,
                detail_url=MissionFetcher.detail_url(mission),
            )

        stats = await cog._sync_missions(
            guild,
            limit=3,
            query=None,
            force_update=False,
        )

        assert stats["skipped"] == 2
        assert stats["created"] == 1
        assert stats["failed"] == 0
        assert all(thread.archived for thread in channel.threads)
        assert channel.created_kwargs[-1]["auto_archive_duration"] == cog.FORUM_AUTO_ARCHIVE_MINUTES

    asyncio.run(run())
