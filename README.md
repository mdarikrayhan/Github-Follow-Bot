# GitHub Follow Bot

> A small Python tool that drives the GitHub REST API to harvest a target user's follower list and bulk follow/unfollow those users from your own account — all from one interactive script.

---

## ⚠️ Disclaimer — read this first

**Mass following/unfollowing (sometimes called "follow farming") may violate [GitHub's Terms of Service](https://docs.github.com/en/site-policy/github-terms/github-terms-of-service) and [Acceptable Use Policies](https://docs.github.com/en/site-policy/acceptable-use-policies/github-acceptable-use-policies).** Automated bulk following can get your account **flagged, rate-limited, or restricted/suspended**, and may trip GitHub's secondary/abuse rate limits.

This project is published **for educational purposes only**. Use it on **your own account, at your own risk**. The author accepts no responsibility for actions taken against your account.

---

## Features

- **One interactive script** — run `python main.py` and choose `harvest`, `follow`, or `unfollow`. There is **no default action**, so you can't trigger a mass change by accident.
- **Harvest followers or following** — pull a public GitHub user's complete **followers** and/or **following** list into a plain-text file, streamed to disk as it arrives so an interrupted run keeps what it already fetched.
- **Fast, parallel harvest** — pages are fetched concurrently by page number (`--workers` at a time, default 10) rather than one-at-a-time, which makes a large harvest rate-limit-bound instead of latency-bound — often a 10×+ speedup on accounts with hundreds of thousands of followers.
- **Resumable harvests** — the output file *is* the checkpoint (one username per line), so a run stopped by `Ctrl-C`, a crash, or a hard API failure picks up from where it left off when you re-run it (no re-fetching, no extra state file).
- **Bulk follow / unfollow** — apply your choice to every username in a saved list, which is auto-discovered from the project root.
- **Rate-limit aware** — a shared client (`ghclient.py`) sets request timeouts and automatically retries with jittered backoff on network errors, 5xx responses, and GitHub's primary/secondary rate limits (honoring `Retry-After`); transient retries are collapsed into a single end-of-run summary instead of spamming the progress bar.
- **Quota visibility** — every harvest and follow/unfollow run prints your remaining API quota at the start and end (e.g. `API quota: 4821/5000 left (resets in ~37m)`) and warns once when it drops below the low-quota threshold, so you can see a primary rate-limit pause coming. The start reading uses GitHub's free `/rate_limit` endpoint (it doesn't consume quota); the threshold is `LOW_QUOTA_THRESHOLD` in `ghclient.py` (default 100).
- **Numbered pagination** — harvest pulls 100 logins per page by page number (`?page=N&per_page=100`), which is what lets pages be fetched in parallel.
- **Progress bars** — a live `tqdm` progress bar for both harvesting and following.
- **Randomized throttling** — each write request is followed by a random pause (default 1–2 seconds, tunable via `--min-delay`/`--max-delay`) so the request pattern looks less robotic.
- **Dry run** — `--dry-run` previews exactly which users a follow/unfollow would affect without sending a single request.
- **Summary output** — a final tally of successes / errors and how many it processed.

## Requirements

- **Python 3** (tested on 3.14)
- Three packages: [`requests`](https://pypi.org/project/requests/), [`python-dotenv`](https://pypi.org/project/python-dotenv/), [`tqdm`](https://pypi.org/project/tqdm/)
- A GitHub personal access token (see [Token setup](#token-setup))

Install the dependencies from `requirements.txt`:

```bash
pip install -r requirements.txt
```

## Token setup

Generate a personal access token at **[github.com/settings/tokens](https://github.com/settings/tokens)**.

| Action | What it does | Token scope needed |
|--------|--------------|--------------------|
| `harvest` | Reads public follower data | **`read:user`** (classic) — a fine-grained read-only token also works |
| `follow` / `unfollow` | Writes to your follows | **`user:follow`** (classic) — or a fine-grained token with the **Followers** user permission set to **Read & write** |

A token with both scopes covers every action. `main.py` reads it from a single `GITHUB_TOKEN` value — the quickest setup is to copy the example file and drop your token in:

```bash
cp .env.example .env
# then edit .env and replace the placeholder with your real token
```

The example file looks like this:

```ini
# main.py loads this .env file.
GITHUB_TOKEN=ghp_xxxxyyyzzz1234567890abcdefg
```

Alternatively, export the variable in your shell instead of using a file:

```bash
export GITHUB_TOKEN=ghp_your_real_token_here
```

> `main.py` refuses to run if `GITHUB_TOKEN` is missing or still a placeholder. `.env` is listed in `.gitignore`, so you can create it without risk of committing your token.

## Usage

Everything runs through one script. Just run it and pick what to do:

```bash
python main.py [-o OUT] [--workers N] [--fresh] [--min-delay S] [--max-delay S] [--dry-run]
```

```text
What do you want to do? [harvest/follow/unfollow]:
```

### harvest — build a follower/following list

Choose `harvest`, then pick the user (Enter accepts the default, `laravel`) and which list to fetch:

```text
GitHub user [laravel]: torvalds
Which list? [followers/following/both] [followers]: both
```

- `followers` → writes `<user>_followers.txt`
- `following` → writes `<user>_following.txt`
- `both` → writes both files

Pages are fetched **in parallel** — `--workers` at a time (default 10) — and streamed one-per-line to disk as they arrive. Pass `-o OUT` to override the filename for a single list (ignored when harvesting `both`). These `*_followers.txt` / `*_following.txt` files are git-ignored — they're generated data, kept out of version control.

**Fast and resumable.** Concurrent page fetching keeps a big harvest pinned against GitHub's rate limit (≈5,000 requests/hour) rather than crawling one slow page at a time. Even so, a million-entry account is a multi-hour job and GitHub returns intermittent `500`s on deep pages (retried automatically). The output file is its own checkpoint — one username per line — so if the run stops (`Ctrl-C`, a crash, or a page that fails every retry), **just run the same harvest again and it resumes** from `lines ÷ 100 + 1` (appending, not re-fetching). A one-line summary reports how many transient server errors were retried, e.g.:

```text
Saved 312900 usernames to torvalds_following.txt (recovered from 47 transient server errors)
```

Tune concurrency with `--workers N` (raise it for more speed, lower it if you hit secondary rate limits); use `--fresh` to ignore saved progress and start the list over.

> **Heads-up on huge accounts:** GitHub's followers/following endpoints degrade on very deep pagination, so an account with, say, 500k+ following may not be fully retrievable via the REST API at all — resume gets you as far as reliably possible. Also, a crash can leave up to ~100 duplicate usernames at the resume seam; this is harmless, since `follow`/`unfollow` de-duplicate the list when reading it.

### follow / unfollow — act on a list

Choose `follow` or `unfollow` and it finds the target list itself:

```text
10706 users to follow from laravel_followers.txt.
```

The **target file** is discovered automatically from the project root by matching the regex `.+_follow(ers|ing).txt$` — so it picks up both harvested followers and following lists. If exactly one matches it is used; if several match you're asked to pick one; if none match it tells you to `harvest` first.

```bash
python main.py --dry-run                # preview the follow/unfollow without sending any requests
python main.py --min-delay 2 --max-delay 4   # slow things down
```

Blank lines and duplicate usernames in the file are ignored. Between requests the run pauses a random time in `[--min-delay, --max-delay]` seconds (default 1–2) to avoid a robotic, evenly-spaced request pattern, then prints a final summary:

```text
Done. followed=42 errors=0 (processed 42/42)
```

Press `Ctrl-C` to stop early — it exits cleanly and still prints the summary of what it managed to do.

Under the hood, each request uses API version `2022-11-28`:

- follow → `PUT /user/following/{username}`
- unfollow → `DELETE /user/following/{username}`

A `204` is treated as success; anything else is reported inline as `error: <status> <message>` and counted in the `errors` tally. Transient failures (network errors, 5xx, rate limits) are retried automatically before being counted as an error.

## Project structure

| File | Purpose |
|------|---------|
| `main.py` | The whole tool. Prompts for `harvest`/`follow`/`unfollow`; harvests a user's followers or acts on the auto-discovered `*_followers.txt` list. |
| `ghclient.py` | Shared client: token loading, the authenticated session, and the rate-limit/retry request wrapper. |
| `requirements.txt` | The three third-party dependencies. |
| `*_followers.txt` / `*_following.txt` | Harvested username lists — generated by `harvest`, git-ignored (not committed). |
| `.env.example` | Template env file showing the expected `GITHUB_TOKEN` format. Copy to `.env`. |
| `.gitignore` | Standard Python ignore template; notably ignores `.env`. |
| `.gitattributes` | `* text=auto` — auto text detection and LF normalization. |

## Notes & rough edges

1. **Explicit action only.** `main.py` always asks `harvest`/`follow`/`unfollow` interactively — there is no command-line default, by design, since follow/unfollow are bulk and hard to reverse. Use `--dry-run` first to confirm the action and the auto-selected target list.
2. **Point-in-time snapshot.** A harvested follower list reflects the moment you ran `harvest`; people follow/unfollow constantly, so re-harvest if you need it fresh.
3. **It's slow, by design.** The random `--min-delay`/`--max-delay` pause between every request (default 1–2s) is a courtesy rate-limit guard. Processing ~10,000 accounts takes **multiple hours**. The shared client backs off and retries when GitHub signals a rate limit, but a large run can still trip GitHub's secondary/abuse limits (see the [Disclaimer](#️-disclaimer--read-this-first)) — raise the delay range if you see repeated waits.
4. **Resuming a write run.** If a follow/unfollow run is interrupted, re-running it re-sends requests for users already processed. Re-following someone returns `204` harmlessly, but it consumes rate-limit budget; trim the list if you need to resume precisely.

## License

No license file is included in this repository, so no usage rights are granted by default. If you intend to reuse or redistribute this code, add an explicit license. Provided **as-is, for educational purposes, with no warranty**.
