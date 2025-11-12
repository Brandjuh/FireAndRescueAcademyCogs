# Forum Thread Mover

Een Red-DiscordBot V3 cog om conversaties van text channels naar forum channels te verplaatsen, waarbij message volgorde, attachments, embeds en authorship behouden blijven.

## Features

- ✅ Verplaats hele conversaties naar forum channels
- ✅ Behoud message volgorde en timestamps
- ✅ Re-upload attachments (met fallback voor grote bestanden)
- ✅ Recreëer embeds waar mogelijk
- ✅ Reply context wordt behouden
- ✅ Forum tags support (optioneel en verplicht)
- ✅ Progress tracking met real-time updates
- ✅ Automatische retry logic bij API errors
- ✅ Comprehensive logging
- ✅ Permission checks
- ✅ Rate limit protection

## Installatie

### Via GitHub (aanbevolen)

```bash
[p]repo add forumthreadmover https://github.com/YOUR_USERNAME/YOUR_REPO
[p]cog install forumthreadmover forumthreadmover
[p]load forumthreadmover
```

### Handmatig

1. Download de hele `forumthreadmover` folder
2. Plaats deze in je Red cogs directory
3. Load de cog:
```bash
[p]load forumthreadmover
```

## Configuratie

De cog heeft de volgende hardcoded settings (pas deze aan in `forumthreadmover.py` indien nodig):

- **Admin Role ID**: `544117282167586836` - Alleen leden met deze rol kunnen de commands gebruiken
- **Log Channel ID**: `668874839012016170` - Waar alle acties worden gelogd
- **Throttle Delay**: `1.5` seconden tussen posts (rate limit protection)
- **Max Retries**: `3` pogingen bij API errors

## Commands

### `[p]movequestion`

Verplaats een conversatie van een text channel naar een forum channel.

**Syntax:**
```
[p]movequestion <message_id> <count> <forum_channel> [title...] [--tag "Tag Name"]
```

**Arguments:**
- `message_id`: Het ID van het start bericht (de vraag)
- `count`: Aantal berichten na het start bericht om te verplaatsen
- `forum_channel`: Het doel forum channel (mention of ID)
- `title`: Optionele titel voor het forum topic (default: eerste 80 chars van de vraag)
- `--tag`: Optionele forum tag om toe te passen (gebruik quotes bij spaties)

**Voorbeelden:**
```
[p]movequestion 1437763879160647740 10 #helpdesk Purpose of Own Vehicle Class
[p]movequestion 1437763879160647740 10 #helpdesk --tag "Bug Report" Purpose of Own Vehicle Class
[p]movequestion 1437763879160647740 10 #helpdesk Title Here --tag Question
```

**Forum Tags:**
- Als een forum verplichte tags heeft, moet je `--tag` gebruiken
- De bot toont beschikbare tags als je een ongeldige tag opgeeft
- Tag names zijn case-insensitive
- Gebruik quotes rondom tag names met spaties

**Wat er gebeurt:**
1. Bot fetcht het originele bericht en de volgende X berichten
2. Creëert een nieuw forum topic met de gegeven titel
3. Post het eerste bericht als main post
4. Post alle volgende berichten als replies (met throttling)
5. Toont progress updates in het originele kanaal
6. Bij succes: "This discussion was moved to [channel]"

### `[p]topictitle`

Verander de titel van een forum thread.

**Syntax:**
```
[p]topictitle <thread_link_or_id> <new_title>
```

**Arguments:**
- `thread`: Link of ID van het forum thread
- `new_title`: De nieuwe titel

**Voorbeeld:**
```
[p]topictitle https://discord.com/channels/123/456/789 New Title Here
```

### `[p]moveinto`

Voeg berichten toe aan een bestaand forum thread.

**Syntax:**
```
[p]moveinto <thread_link_or_id> <message_id> <count>
```

**Arguments:**
- `thread`: Link of ID van het doel forum thread
- `message_id`: Het ID van het start bericht
- `count`: Aantal berichten na het start bericht

**Voorbeeld:**
```
[p]moveinto https://discord.com/channels/123/456/789 1437763879160647740 5
```

## Permissions

De bot heeft de volgende permissions nodig:

**In source channel:**
- View Channel
- Read Message History

**In target forum channel:**
- Create Public Threads (voor forum posts)
- Send Messages
- Attach Files
- Embed Links
- Manage Threads

## Content Handling

### Text
- Volledige message text wordt 1:1 gekopieerd
- Mentions worden disabled (geen pings)
- Elk bericht begint met `**@username** • <timestamp>`

### Attachments
- Bestanden worden opnieuw geüpload
- Bestanden >25MB → CDN URL wordt gepost
- Bij download failures → link naar origineel bericht

### Embeds
- Rich embeds worden gerecreerd met:
  - Title, description, URL
  - Author, fields, footer
  - Image, thumbnail, timestamp
- Link previews en special embeds → URL wordt gepost
- Interactive components (buttons/menus) → notitie met link naar origineel

### Reply Context
Als een bericht een reply was:
```
↪️ In reply to @user: "eerste 80 characters..."
```

## Formatting

Elk bericht krijgt de volgende structuur:
```
**@username** • <timestamp>

[Reply context indien applicable]

[Message content]

[Attachments/Embeds]

━━━━━━━━━━━━━━━━━━━━
(Moved from #channel on YYYY-MM-DD)
```

## Error Handling

De cog heeft robust error handling:

- **Message niet gevonden**: Error message, geen actie
- **Permissions ontbreken**: Lijst van missende permissions
- **Rate limits**: Automatische throttling (1.5s delay)
- **API errors**: 3 retry pogingen met 2s delay
- **Partial failures**: Cleanup en warning
- **Thread detection**: Warning als bericht al in thread zit

## Logging

Alle acties worden gelogd naar channel `668874839012016170`:

- Succesvolle moves (groen)
- Failed moves met error details (rood)
- Title changes (blauw)

Log embed format:
```
Moderator: @user
From: #source-channel
To: #forum-channel
Thread: [Title](link)
Messages: X
```

## Safety Features

- Maximum 100 berichten per move (rate limit protection)
- Automatische throttling tussen posts
- Retry logic bij API errors
- Comprehensive permission checks
- No-ping policy (allowed_mentions=none)
- Message content truncation bij >2000 chars

## Limitations

- Originele berichten blijven staan (niet verwijderd)
- Interactive components (buttons/menus) kunnen niet worden gekloneerd
- Special embeds (video previews, etc.) worden links
- Bot moet owner zijn van forum post voor full thread management

## Changelog

### v1.1.0
- Added forum tag support with `--tag` flag
- Automatic detection of required tags
- Tag name validation with available tags list
- Case-insensitive tag matching

### v1.0.0
- Initial release
- `movequestion` command
- `topictitle` command
- `moveinto` command
- Progress tracking
- Retry logic
- Comprehensive logging

## Support

Voor vragen of bugs, open een issue op GitHub.

## License

MIT License - vrij te gebruiken en aan te passen.
