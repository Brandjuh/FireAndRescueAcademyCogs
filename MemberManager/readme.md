# 🔧 MemberManager Fixes - Complete Documentation

## Versie 1.1.0 - Complete Rewrite

Alle gerapporteerde problemen zijn opgelost in deze versie.

---

## 🐛 Opgeloste Problemen

### 1. ✅ Contribution wordt niet weergegeven

**Probleem:**
- Contribution rate werd niet correct weergegeven in de overview
- Kon crashen als de data `None` was

**Oplossing:**
- Toegevoegd: Error handling in `_build_member_data()` 
- Toegevoegd: Type conversie naar float voor contribution_rate
- Toegevoegd: Fallback display "*No data*" als contribution_rate None is
- Verbeterd: `format_contribution_trend()` error handling in views.py

**Code locatie:** `membermanager.py` regel ~235-250, `views.py` regel ~285-295

---

### 2. ✅ Synchronisatie tussen MemberSync en MemberManagement

**Probleem:**
- Leden staan als "Not Verified" terwijl ze wel gelinkt zijn in de database
- `is_verified` werd alleen gezet als de Discord member de verified role had
- Link status werd niet gecontroleerd

**Oplossing:**
- **HOOFDFIX:** `is_verified` wordt nu gezet als `link_status == 'approved'` 
- Role check is nu secundair (geeft alleen warning als role mist)
- Link status wordt altijd opgehaald en getoond in overview
- Betere logging voor missing role situaties

**Code locatie:** `membermanager.py` regel ~210-240

**Nieuwe logica:**
```python
# Voor: is_verified = (role in member.roles)
# Nu: is_verified = (link_status == 'approved')
if link and link.get("status") == "approved":
    data.is_verified = True
    # Role check is nu optioneel/warning
```

---

### 3. ✅ Infractions staan niet in de lijst

**Probleem:**
- Infractions werden niet weergegeven (crash of lege lijst)
- Geen error handling bij database queries
- View kon crashen bij ontbrekende data

**Oplossing:**
- Toegevoegd: Try-except blocks in `get_infractions()` method
- Toegevoegd: Error handling in `get_infractions_embed()`
- Toegevoegd: Graceful fallback messages bij errors
- Verbeterd: Infractions worden nu grouped per platform (Discord/MC)

**Code locatie:** `views.py` regel ~400-480, `membermanager.py` regel ~260-280

---

### 4. ✅ Meerdere gebruikers kunnen hetzelfde scherm gebruiken

**Probleem:**
- `interaction_check()` returnde altijd `True`
- Iedereen kon buttons van elkaars panels gebruiken

**Oplossing:**
- **HOOFDFIX:** `invoker_id` parameter toegevoegd aan `MemberOverviewView`
- `interaction_check()` controleert nu of `interaction.user.id == invoker_id`
- Gebruikers krijgen duidelijke error message als ze proberen andermans panel te gebruiken

**Code locatie:** `views.py` regel ~50-60, `membermanager.py` regel ~335

**Nieuwe code:**
```python
async def interaction_check(self, interaction: discord.Interaction) -> bool:
    if interaction.user.id != self.invoker_id:
        await interaction.response.send_message(
            "❌ This is not your member info panel.",
            ephemeral=True
        )
        return False
    return True
```

---

### 5. ✅ Note editing zonder editor tracking + Audit Log

**Probleem:**
- `update_note()` zette wel `updated_by`, maar dit werd niet getoond
- Geen history van wie wat heeft bewerkt
- Geen complete audit trail

**Oplossing:**
- **NIEUWE KOLOM:** `updated_by_name` toegevoegd aan notes table
- **NIEUWE FUNCTIE:** `log_action()` method in database.py
- **NIEUWE TAB:** "Audit" tab in de view met complete history
- Note edits tonen nu: "✏️ *Edited by [naam] [tijd]*"
- Alle acties worden gelogd: note_created, note_edited, note_deleted, etc.

**Code locatie:** 
- `database.py` regel ~200-250 (audit log systeem)
- `views.py` regel ~100-120 (audit tab), regel ~340-360 (edit display)

**Nieuwe functies:**
```python
# Audit logging
await self.db.log_action(
    guild_id=guild_id,
    action_type="note_edited",
    action_target=ref_code,
    actor_id=updated_by,
    actor_name=updated_by_name,
    old_value=old_text[:100],
    new_value=new_text[:100]
)

# Display in notes
if note.get("updated_by"):
    updated_by_name = note.get("updated_by_name", "Unknown")
    updated_at = format_timestamp(note.get("updated_at", 0), "R")
    lines.append(f"✏️ *Edited by {updated_by_name} {updated_at}*")
```

---

## 🆕 Nieuwe Features

### Audit Log Tab
Complete geschiedenis van alle acties op een member account:
- Note creatie, edits, deletions
- Infraction toevoegingen, revocations
- Toont actor, timestamp, old/new values
- Emoji's per action type voor duidelijkheid

### Verbeterde Error Handling
- Alle database queries hebben try-except blocks
- Graceful fallbacks bij missing data
- Duidelijke error messages voor gebruikers
- Logging van alle errors voor debugging

### Better Link Status Display
- Link status wordt altijd getoond in overview
- Duidelijk onderscheid tussen "approved", "pending", "denied"
- Warning in logs als linked maar role mist

---

## 📊 Database Schema Updates

### Notes Table - Nieuwe Kolommen
```sql
ALTER TABLE notes ADD COLUMN updated_by_name TEXT;
```

### Audit Log Table - Nieuwe Tabel
```sql
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id INTEGER NOT NULL,
    discord_id INTEGER,
    mc_user_id TEXT,
    action_type TEXT NOT NULL,
    action_target TEXT NOT NULL,
    actor_id INTEGER NOT NULL,
    actor_name TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    timestamp INTEGER NOT NULL,
    metadata TEXT
);
```

**Indices:**
```sql
CREATE INDEX idx_audit_discord ON audit_log(discord_id);
CREATE INDEX idx_audit_mc ON audit_log(mc_user_id);
CREATE INDEX idx_audit_target ON audit_log(action_target);
```

---

## 🔄 Migratie Instructies

### Stap 1: Backup bestaande database
```bash
cd ~/.local/share/Red-DiscordBot/data/[instance]/cogs/MemberManager/
cp membermanager.db membermanager.db.backup
```

### Stap 2: Stop de bot
```bash
[p]shutdown
```

### Stap 3: Update de cog files
Vervang de volgende files:
- `database.py` ✅
- `membermanager.py` ✅
- `views.py` ✅
- `models.py` ✅

### Stap 4: Start de bot
```bash
# Bot start automatisch en runt database migratie
```

### Stap 5: Verificatie
```bash
[p]member @someone
# Test of alles werkt
# Check de nieuwe "Audit" tab
```

---

## 🧪 Testing Checklist

### Test 1: Verification Status
- [ ] Link een member via MemberSync
- [ ] Check `/member @user` - moet "Verified" tonen
- [ ] Verwijder verified role (maar laat link intact)
- [ ] Check `/member @user` - moet NOG STEEDS "Verified" tonen (omdat link_status='approved')
- [ ] Check bot logs voor warning over missing role

### Test 2: Contribution Display
- [ ] Check member met contribution data
- [ ] Check member zonder contribution data (moet "*No data*" tonen)
- [ ] Moet niet crashen

### Test 3: Infractions
- [ ] Check member met infractions
- [ ] Check member zonder infractions
- [ ] Moet "No active infractions" tonen, niet crashen

### Test 4: Multi-User Protection
- [ ] User A: `/member @someone`
- [ ] User B: Probeer buttons te gebruiken op User A's panel
- [ ] User B moet error krijgen: "This is not your member info panel"

### Test 5: Note Editing + Audit
- [ ] Voeg note toe via "Add Note" button
- [ ] Edit de note via "Edit Note" button
- [ ] Check "Notes" tab - moet tonen: "✏️ *Edited by [naam] [tijd]*"
- [ ] Check "Audit" tab - moet beide acties tonen (created + edited)
- [ ] Check old/new values in audit log

### Test 6: Refresh Button
- [ ] Open member panel
- [ ] Verander iets (bijv. link member in andere tab)
- [ ] Klik "Refresh" button
- [ ] Data moet updaten

---

## 📋 Command Overview

### Member Commands
```bash
# Lookup member info
[p]member @user
[p]member 123456789  # Discord ID
[p]member 987654     # MC ID
[p]member John Doe   # Fuzzy name search

# Search members
[p]membersearch john

# View stats
[p]memberstats
```

### Configuration Commands
```bash
# View config
[p]memberset view

# Set channels
[p]memberset alertchannel #admin-alerts
[p]memberset modlogchannel #mod-log

# Set roles
[p]memberset adminroles @Admin @Leadership
[p]memberset modroles @Moderator

# Set thresholds
[p]memberset threshold 5.0
[p]memberset trendweeks 3

# Enable/disable automation
[p]memberset autocontribution true
[p]memberset autoroledrift true
```

---

## 🔌 Integration Status

### MemberSync ✅
- Link status tracking
- Verified role checking
- Auto-sync wanneer links worden approved

### AllianceScraper ✅
- MC username, role, contribution rate
- Profile links
- Real-time data

### SanctionManager ⚠️
- Basis integratie aanwezig
- Kan worden uitgebreid voor sanction display

---

## 🐞 Known Issues & Workarounds

### Issue: Database locked errors
**Symptoom:** `sqlite3.OperationalError: database is locked`

**Oorzaak:** Meerdere cogs proberen tegelijk te schrijven naar databases

**Workaround:**
1. Zorg dat AllianceScraper en MemberSync async writes gebruiken
2. Gebruik connection pooling
3. Tijdelijk: Verhoog timeout in database connections

### Issue: Oude notes zonder updated_by_name
**Symptoom:** Notes created vóór update tonen "Unknown" als editor

**Oplossing:** Dit is normaal - alleen nieuwe edits tracken de naam

---

## 📝 Changelog

### Version 1.1.0 (2025-01-XX)
- ✅ Fixed verification status check (link_status based)
- ✅ Fixed contribution display with error handling
- ✅ Fixed infractions not showing (error handling)
- ✅ Fixed multi-user panel access (invoker_id check)
- ✅ Added audit log system (complete history)
- ✅ Added editor name tracking in notes
- ✅ Added new "Audit" tab in UI
- ✅ Improved error handling everywhere
- ✅ Better integration with MemberSync database
- ✅ Added refresh button functionality

### Version 1.0.0 (Original)
- ⚠️ Had verification status bug
- ⚠️ Missing contribution error handling
- ⚠️ Missing infraction error handling
- ⚠️ No multi-user protection
- ⚠️ No audit trail

---

## 🆘 Troubleshooting

### Problem: "Error loading notes/infractions"
**Check:**
1. Database permissions: `ls -la ~/.local/share/Red-DiscordBot/data/[instance]/cogs/MemberManager/`
2. Database integrity: Open with sqlite3 and run `.schema`
3. Bot logs: Check for detailed error messages

**Fix:**
```bash
# Rebuild database schema
[p]unload membermanager
[p]load membermanager
```

### Problem: Members show as "Not Verified" but are linked
**Check:**
1. MemberSync database: `SELECT * FROM links WHERE discord_id=...`
2. Link status: Should be 'approved'
3. Bot logs: Look for verification checks

**Fix:**
1. Re-approve link: `[p]membersync link @user [mc_id]`
2. Or check MemberSync configuration

### Problem: Audit log is empty
**Reason:** Audit log only tracks actions AFTER v1.1.0 installation

**Not a bug:** Historical data before update is not retroactively logged

### Problem: "This is not your member info panel"
**Reason:** Someone else opened that panel

**Solution:** Open your own with `/member @user`

---

## 💡 Tips & Best Practices

### For Admins
1. **Regular backups:** Backup `membermanager.db` daily
2. **Monitor logs:** Check for verification warnings
3. **Review audit logs:** Periodically check `/member` audit tabs
4. **Test after updates:** Always test on dev server first

### For Moderators
1. **Use refresh:** After linking members, hit refresh button
2. **Check audit trail:** Before editing notes, check who made them
3. **Add context:** Use infraction_ref when adding notes
4. **Set expiry:** Consider expiry dates for temporary notes

### For Users
1. **Link accounts:** Make sure you're verified via `/verify`
2. **Contact mods:** If your status is wrong, ask for re-verification
3. **Be patient:** Panel times out after 5 minutes

---

## 🔮 Future Improvements (Roadmap)

### Planned Features
- [ ] Note templates for common situations
- [ ] Bulk actions (e.g., expire multiple infractions)
- [ ] Advanced search filters
- [ ] Export member data to PDF/CSV
- [ ] Watchlist auto-notifications
- [ ] Integration with Red's modlog
- [ ] Role drift auto-fix
- [ ] Contribution alerts automation
- [ ] Custom note categories/tags
- [ ] Member comparison view

### Under Consideration
- [ ] Discord message context menu integration
- [ ] Automated warning escalation
- [ ] Member activity timeline
- [ ] Points/reputation system
- [ ] Achievement badges

---

## 📞 Support

### Getting Help
1. Check this README first
2. Check bot logs: `tail -f ~/.local/share/Red-DiscordBot/logs/red.log | grep MemberManager`
3. Enable debug mode: `[p]set loglevel debug`
4. Ask in your Discord support channel

### Reporting Bugs
When reporting bugs, include:
- Bot version and Python version
- Full error message from logs
- Steps to reproduce
- Expected vs actual behavior
- Screenshots if applicable

### Contributing
Pull requests welcome! Please:
- Follow existing code style
- Add tests for new features
- Update documentation
- Test thoroughly before submitting

---

## 📜 License & Credits

**Author:** Fire & Rescue Academy Development Team  
**Version:** 1.1.0  
**License:** MIT  

**Dependencies:**
- Red-DiscordBot >= 3.5
- discord.py >= 2.0
- aiosqlite >= 0.17
- Python >= 3.8

**Special Thanks:**
- MemberSync cog for link management
- AllianceScraper cog for MC data
- Red-DiscordBot community

---

## ✅ Installation Complete!

Your MemberManager is now fully updated with all fixes applied. 

**Next steps:**
1. Test all functionality using the checklist above
2. Review the new Audit tab features
3. Configure automation settings if desired
4. Train your moderators on the new features

**Questions?** Check the troubleshooting section or ask for help in your support channel.

---

**Happy Managing! 🎉**
