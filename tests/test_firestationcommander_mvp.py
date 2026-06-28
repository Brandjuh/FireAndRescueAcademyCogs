import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import discord

from firestationcommander.constants import DEFAULT_START_CASH
from firestationcommander.firestationcommander import FireStationCommander
from firestationcommander.models import Player
from firestationcommander.views.dashboard import DashboardView


class _Response:
    def __init__(self):
        self.edited = None
        self.messages = []

    async def edit_message(self, **kwargs):
        self.edited = kwargs

    async def send_message(self, content=None, **kwargs):
        self.messages.append((content, kwargs))

    def is_done(self):
        return False


class _Interaction:
    def __init__(self, guild_id=100, user_id=200):
        self.guild = SimpleNamespace(id=guild_id)
        self.user = SimpleNamespace(id=user_id)
        self.response = _Response()


class _Message:
    def __init__(self):
        self.edited = None

    async def edit(self, **kwargs):
        self.edited = kwargs


def _embed_fields(embed):
    return {field["name"]: field["value"] for field in embed.fields}


async def _make_loaded_cog(tmp_path):
    cog = FireStationCommander(bot=None, db_path=tmp_path)
    await cog.cog_load()
    return cog


async def _seed_player(cog):
    player, created = await cog.db.get_or_create_player(100, 200)
    await cog.db.create_station(player.id, "Test Station", 2, 8)
    if created:
        await cog._seed_starter_assets(player.id)
    return player


def test_starting_player_gets_station_and_starter_assets(tmp_path):
    async def run():
        cog = await _make_loaded_cog(tmp_path)
        try:
            player = await _seed_player(cog)

            station = await cog.db.get_station(player.id)
            vehicles = await cog.db.list_vehicles(player.id)
            personnel = await cog.db.list_personnel(player.id)
            equipment = await cog.db.list_equipment(player.id)
            training_keys = {
                training
                for member in personnel
                for training in await cog.db.trainings_for_personnel(member.id)
            }

            assert station is not None
            assert station.garage_slots == 2
            assert [vehicle.template_key for vehicle in vehicles] == ["ts"]
            assert len(personnel) == 6
            assert {row["template_key"] for row in equipment} == {
                "breathing_apparatus",
                "thermal_camera",
                "hose_pack",
                "aed",
            }
            assert "basic_firefighting" in training_keys
            assert "pump_operator" in training_keys
        finally:
            await cog.cog_unload()

    asyncio.run(run())


def test_level_one_incident_selection_only_uses_level_one_templates(tmp_path):
    async def run():
        cog = await _make_loaded_cog(tmp_path)
        try:
            player = Player(
                id=1,
                guild_id=100,
                user_id=200,
                cash=DEFAULT_START_CASH,
                reputation=0,
                command_level=1,
                xp=0,
                safety_score=75,
                morale_score=75,
            )

            selected_keys = {
                cog.incident_service.choose_template(player)["key"]
                for _ in range(50)
            }

            assert selected_keys
            assert selected_keys <= {"dumpster_fire", "outdoor_fire", "kitchen_fire"}
        finally:
            await cog.cog_unload()

    asyncio.run(run())


def test_incident_embed_shows_time_limit_and_requirements(tmp_path):
    async def run():
        cog = await _make_loaded_cog(tmp_path)
        try:
            player = await _seed_player(cog)
            template = cog.incidents_by_key["dumpster_fire"]
            incident = await cog.db.create_incident(
                100,
                player.id,
                template,
                expires_at=cog.incident_service.expires_at(template),
            )

            embed = cog._build_incident_embed(incident)
            fields = _embed_fields(embed)

            assert fields["Time limit"] == "20 minutes"
            assert "Vehicles: TS" in fields["Requirements"]
            assert "Training: Basic Firefighting" in fields["Requirements"]
            assert "Capabilities: fire, water" in fields["Requirements"]
        finally:
            await cog.cog_unload()

    asyncio.run(run())


def test_legacy_incident_keys_render_with_english_text(tmp_path):
    async def run():
        cog = await _make_loaded_cog(tmp_path)
        try:
            player = await _seed_player(cog)
            legacy_template = dict(cog.incidents_by_key["dumpster_fire"])
            legacy_template["key"] = "containerbrand"
            legacy_template["title"] = "Containerbrand"
            legacy_template["required_trainings"] = ["basis_brandbestrijding"]
            incident = await cog.db.create_incident(
                100,
                player.id,
                legacy_template,
                expires_at=cog.incident_service.expires_at(legacy_template),
            )

            embed = cog._build_incident_embed(incident)
            fields = _embed_fields(embed)

            assert embed.kwargs["title"] == "Incident: Dumpster Fire"
            assert "Training: Basic Firefighting" in fields["Requirements"]
        finally:
            await cog.cog_unload()

    asyncio.run(run())


def test_starter_response_scores_container_fire_strongly(tmp_path):
    async def run():
        cog = await _make_loaded_cog(tmp_path)
        try:
            player = await _seed_player(cog)
            vehicles = await cog.db.list_vehicles(player.id)
            personnel = await cog.db.list_personnel(player.id)
            equipment = await cog.db.list_equipment(player.id)
            training_map = {
                member.id: await cog.db.trainings_for_personnel(member.id)
                for member in personnel
            }
            template = cog.incidents_by_key["dumpster_fire"]

            score = cog.incident_service.calculate_score(
                template=template,
                selected_vehicles=vehicles,
                available_personnel=personnel,
                equipment_rows=equipment,
                equipment_templates=cog.equipment_by_key,
                training_map=training_map,
                random_modifier=0,
            )

            assert score.score >= 90
            assert score.breakdown["vehicles"] == 100
            assert score.breakdown["equipment"] == 100
        finally:
            await cog.cog_unload()

    asyncio.run(run())


def test_legacy_training_keys_still_count_for_scoring(tmp_path):
    async def run():
        cog = await _make_loaded_cog(tmp_path)
        try:
            player = await _seed_player(cog)
            vehicles = await cog.db.list_vehicles(player.id)
            personnel = await cog.db.list_personnel(player.id)
            equipment = await cog.db.list_equipment(player.id)
            training_map = {member.id: ["basis_brandbestrijding"] for member in personnel}
            template = cog.incidents_by_key["dumpster_fire"]

            score = cog.incident_service.calculate_score(
                template=template,
                selected_vehicles=vehicles,
                available_personnel=personnel,
                equipment_rows=equipment,
                equipment_templates=cog.equipment_by_key,
                training_map=training_map,
                random_modifier=0,
            )

            assert score.breakdown["training"] == 100
        finally:
            await cog.cog_unload()

    asyncio.run(run())


def test_dashboard_view_rejects_non_owner_ephemerally():
    async def run():
        view = DashboardView(cog=object(), owner_id=200)
        interaction = _Interaction(user_id=999)

        allowed = await view.interaction_check(interaction)

        assert allowed is False
        assert interaction.response.messages == [
            ("Only the station commander can use this dashboard.", {"ephemeral": True})
        ]

    asyncio.run(run())


def test_dashboard_view_errors_are_reported_ephemerally():
    async def run():
        view = DashboardView(cog=object(), owner_id=200)
        interaction = _Interaction(user_id=200)

        await view.on_error(interaction, RuntimeError("boom"), item=object())

        assert len(interaction.response.messages) == 1
        content, kwargs = interaction.response.messages[0]
        assert "FireStationCommander menu hit an error while handling the button" in content
        assert kwargs == {"ephemeral": True}

    asyncio.run(run())


def test_dashboard_view_timeout_disables_controls_and_edits_message():
    async def run():
        view = DashboardView(cog=object(), owner_id=200)
        button = discord.ui.Button(label="Vehicles", style=discord.ButtonStyle.danger)
        view.children.append(button)
        view.message = _Message()

        await view.on_timeout()

        assert button.disabled is True
        assert button.style == discord.ButtonStyle.secondary
        assert view.message.edited == {
            "content": "This menu timed out. Open `[p]fsc status` again to continue.",
            "view": view,
        }

    asyncio.run(run())


def test_dispatch_resolves_incident_updates_assets_and_report(tmp_path):
    async def run():
        cog = await _make_loaded_cog(tmp_path)
        try:
            player = await _seed_player(cog)
            vehicle = (await cog.db.list_vehicles(player.id))[0]
            template = cog.incidents_by_key["dumpster_fire"]
            incident = await cog.db.create_incident(
                100,
                player.id,
                template,
                expires_at=cog.incident_service.expires_at(template),
            )
            interaction = _Interaction()

            with patch("firestationcommander.services.incidents.random.randint", return_value=0):
                await cog.finish_incident_dispatch(interaction, incident.id, [vehicle.id])

            updated_player = await cog.db.get_player_by_id(player.id)
            updated_vehicle = (await cog.db.list_vehicles(player.id))[0]
            report = await cog.db.latest_report(player.id)
            completed = await cog.db.get_incident(incident.id)

            assert updated_player.cash > DEFAULT_START_CASH
            assert updated_player.xp > 0
            assert updated_vehicle.condition_score < 100
            assert updated_vehicle.fuel < 100
            assert report is not None
            assert report.score >= 90
            assert completed.status == "completed"
            assert interaction.response.edited["view"] is None
            assert interaction.response.edited["embed"].kwargs["title"] == "Incident report"
        finally:
            await cog.cog_unload()

    asyncio.run(run())


def test_maintenance_requires_enough_cash(tmp_path):
    async def run():
        cog = await _make_loaded_cog(tmp_path)
        try:
            player = await _seed_player(cog)
            vehicle = (await cog.db.list_vehicles(player.id))[0]
            template = cog.incidents_by_key["dumpster_fire"]
            incident = await cog.db.create_incident(
                100,
                player.id,
                template,
                expires_at=cog.incident_service.expires_at(template),
            )
            with patch("firestationcommander.services.incidents.random.randint", return_value=0):
                await cog.finish_incident_dispatch(_Interaction(), incident.id, [vehicle.id])
            await cog.db.conn.execute("UPDATE players SET cash = 0 WHERE id = ?", (player.id,))
            await cog.db.conn.commit()
            interaction = _Interaction()

            await cog.repair_all_from_interaction(interaction)

            assert len(interaction.response.messages) == 1
            content, kwargs = interaction.response.messages[0]
            assert content.startswith("Maintenance costs ")
            assert kwargs == {"ephemeral": True}
        finally:
            await cog.cog_unload()

    asyncio.run(run())


def test_maintenance_embed_lists_worn_equipment(tmp_path):
    async def run():
        cog = await _make_loaded_cog(tmp_path)
        try:
            player = await _seed_player(cog)
            equipment = await cog.db.list_equipment(player.id)
            await cog.db.conn.execute(
                "UPDATE equipment SET condition_score = 60 WHERE id = ?",
                (equipment[0]["id"],),
            )
            await cog.db.conn.commit()

            embed = await cog._build_maintenance_embed(player.id)
            fields = _embed_fields(embed)

            assert "Equipment: Breathing Apparatus" in fields
            assert fields["Equipment: Breathing Apparatus"] == "Condition 60% | Cost 400"
            assert fields["Total repair cost"] == "400"
        finally:
            await cog.cog_unload()

    asyncio.run(run())


def test_maintenance_repairs_worn_vehicle_and_spends_cash(tmp_path):
    async def run():
        cog = await _make_loaded_cog(tmp_path)
        try:
            player = await _seed_player(cog)
            vehicle = (await cog.db.list_vehicles(player.id))[0]
            template = cog.incidents_by_key["dumpster_fire"]
            incident = await cog.db.create_incident(
                100,
                player.id,
                template,
                expires_at=cog.incident_service.expires_at(template),
            )

            with patch("firestationcommander.services.incidents.random.randint", return_value=0):
                await cog.finish_incident_dispatch(_Interaction(), incident.id, [vehicle.id])

            after_incident = await cog.db.get_player_by_id(player.id)
            await cog.repair_all_from_interaction(_Interaction())
            repaired_player = await cog.db.get_player_by_id(player.id)
            repaired_vehicle = (await cog.db.list_vehicles(player.id))[0]

            assert repaired_vehicle.condition_score == 100
            assert repaired_vehicle.fuel == 100
            assert repaired_vehicle.damage == 0
            assert repaired_player.cash < after_incident.cash
        finally:
            await cog.cog_unload()

    asyncio.run(run())
