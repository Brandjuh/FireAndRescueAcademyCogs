# Top Players Cog

Commands:
- `[p]tplayers setdb <path>` — set the sqlite DB path (AllianceScraper DB)
- `[p]tplayers settz <IANA tz>` — default `America/New_York`
- `[p]tplayers settopn <N>` — default 10
- `[p]tplayers preview [YYYY-MM-DD|daily|today]` — show daily top list
- `[p]tplayers debug [YYYY-MM-DD]` — print window + quick sample

The cog computes per-member credits delta between:
- last snapshot **< START** (NY 00:00)
- last snapshot **≤ END** (NY 23:50)

It works directly on `members_history` and joins `members_current` to get names.
