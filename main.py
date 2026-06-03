"""GitHub Follow Bot.

Run with no arguments and pick what to do:
  * harvest  — fetch a user's followers and/or following into
               ``<user>_followers.txt`` / ``<user>_following.txt``
  * follow   — follow every username in a saved list
  * unfollow — unfollow every username in a saved list

The follow/unfollow lists are discovered automatically from the project root
(any ``*_followers.txt`` or ``*_following.txt`` file).
"""
import argparse
import json
import os
import random
import re
import sys
import time

import requests
from tqdm import tqdm

import ghclient

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
TARGETS_PATTERN = re.compile(r".+_follow(?:ers|ing)\.txt$")


# --------------------------------------------------------------------------- #
# Interactive prompts
# --------------------------------------------------------------------------- #
def _ask(prompt):
    try:
        return input(prompt).strip()
    except (EOFError, KeyboardInterrupt):
        sys.exit("\nCancelled.")


def prompt_choice():
    """Ask what to do: harvest a list, or follow/unfollow one."""
    while True:
        choice = _ask("What do you want to do? [harvest/follow/unfollow]: ").lower()
        if choice in ("harvest", "follow", "unfollow"):
            return choice
        print("Please type 'harvest', 'follow', or 'unfollow'.")


def prompt_user(default="laravel"):
    """Ask interactively which user to harvest."""
    answer = _ask(f"GitHub user [{default}]: ")
    return answer or default


def prompt_list_kind(default="followers"):
    """Ask which list to harvest: followers, following, or both."""
    while True:
        kind = _ask(f"Which list? [followers/following/both] [{default}]: ").lower()
        kind = kind or default
        if kind in ("followers", "following", "both"):
            return kind
        print("Please type 'followers', 'following', or 'both'.")


# --------------------------------------------------------------------------- #
# Harvest: fetch a user's followers and/or following into files
# --------------------------------------------------------------------------- #
def get_user(session, user):
    """Return `user`'s public profile JSON (includes follower/following counts)."""
    response = ghclient.request(session, "GET", f"{ghclient.API_BASE}/users/{user}")
    response.raise_for_status()
    return response.json()


def iter_pages(session, user, kind, start_url=None, on_retry=None):
    """Yield (logins, next_url) per page of `user`'s `kind` list.

    Starts at `start_url` when resuming, otherwise the first page. Each yielded
    `next_url` is the cursor to checkpoint once the page is safely on disk.
    Idempotent GETs get a higher retry budget than the default.
    """
    url = start_url or f"{ghclient.API_BASE}/users/{user}/{kind}"
    params = None if start_url else {"per_page": 100}
    while url:
        response = ghclient.request(session, "GET", url, params=params,
                                    max_retries=8, on_retry=on_retry)
        response.raise_for_status()
        logins = [entry["login"] for entry in response.json()]
        next_url = response.links.get("next", {}).get("url")
        yield logins, next_url
        url = next_url
        params = None  # subsequent page URLs already carry the query string


class _Counter:
    """A callable that counts how many times it was invoked (for on_retry)."""
    def __init__(self):
        self.count = 0

    def __call__(self):
        self.count += 1


def _load_state(path):
    """Return saved harvest state dict, or None if absent/unreadable."""
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def _save_state(path, data):
    """Atomically write harvest state (temp file + os.replace)."""
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(data, handle)
    os.replace(tmp, path)


def _remove(path):
    """Remove a file if it exists."""
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def _harvest_one(session, user, kind, total, outfile, fresh=False):
    """Harvest one list into `outfile`, resuming from `<outfile>.state` if present."""
    statefile = f"{outfile}.state"
    state = None if fresh else _load_state(statefile)
    resuming = bool(state) and bool(state.get("next_url")) and os.path.exists(outfile)
    if not resuming:
        _remove(statefile)  # stale or fresh start — drop any leftover cursor

    start_url = state["next_url"] if resuming else None
    saved = state["saved"] if resuming else 0

    if resuming:
        print(f"\nResuming {user}'s {kind} from {saved} already saved → {outfile}...")
    else:
        print(f"\n{user} has {total} {kind}, fetching to {outfile}...")

    retries = _Counter()
    try:
        with open(outfile, "a" if resuming else "w", encoding="utf-8") as handle, \
                tqdm(total=total, initial=saved, unit="user") as bar:
            for logins, next_url in iter_pages(session, user, kind,
                                               start_url=start_url, on_retry=retries):
                handle.write("".join(login + "\n" for login in logins))
                handle.flush()
                saved += len(logins)
                bar.update(len(logins))
                _save_state(statefile, {"next_url": next_url, "saved": saved})
    except KeyboardInterrupt:
        print(f"\nStopped — re-run to resume {kind} from {saved} ({outfile}).",
              file=sys.stderr)
        return 130
    except requests.exceptions.HTTPError as exc:
        print(f"\nStopped after an API error ({exc}); re-run to resume {kind} "
              f"from {saved} ({outfile}).", file=sys.stderr)
        return 1

    _remove(statefile)
    summary = f"Saved {saved} usernames to {outfile}"
    if retries.count:
        summary += f" (recovered from {retries.count} transient server errors)"
    print(summary)
    return 0


def harvest(args):
    user = prompt_user()
    kind = prompt_list_kind()
    kinds = ["followers", "following"] if kind == "both" else [kind]

    session = ghclient.make_session(ghclient.get_token())
    try:
        profile = get_user(session, user)
    except requests.exceptions.HTTPError as exc:
        sys.exit(f"Could not fetch '{user}': {exc}")

    # Route retry/quota notices through the bar so they don't corrupt it.
    previous_log = ghclient.log
    ghclient.log = tqdm.write
    try:
        start = ghclient.get_rate_limit(session)
        if start:
            print(start)
        for list_kind in kinds:
            outfile = args.out if (args.out and kind != "both") else f"{user}_{list_kind}.txt"
            rc = _harvest_one(session, user, list_kind, profile.get(list_kind, 0),
                              outfile, fresh=args.fresh)
            if rc != 0:
                return rc
    finally:
        end = ghclient.rate_limit_summary()
        if end:
            print(end)
        ghclient.log = previous_log
    return 0


# --------------------------------------------------------------------------- #
# Follow / unfollow: act on a saved list
# --------------------------------------------------------------------------- #
def _write(session, method, username):
    """PUT/DELETE /user/following/{username}. Return (ok, detail)."""
    response = ghclient.request(
        session, method, f"{ghclient.API_BASE}/user/following/{username}"
    )
    if response.status_code == 204:
        return True, "ok"
    try:
        message = response.json().get("message", "")
    except ValueError:
        message = response.text
    return False, f"{response.status_code} {message[:80]}".strip()


def follow(session, username):
    return _write(session, "PUT", username)


def unfollow(session, username):
    return _write(session, "DELETE", username)


def read_targets(path):
    """Return de-duplicated, non-blank usernames from `path`, in order."""
    with open(path, encoding="utf-8") as handle:
        names = (line.strip() for line in handle)
        return list(dict.fromkeys(name for name in names if name))


def find_targets_file(root=PROJECT_ROOT):
    """Find a harvested list file (`*_followers.txt` / `*_following.txt`) by regex.

    Exits if none match; prompts to choose when more than one matches.
    """
    matches = sorted(n for n in os.listdir(root) if TARGETS_PATTERN.match(n))
    if not matches:
        sys.exit(
            f"No targets file matching /{TARGETS_PATTERN.pattern}/ found in "
            f"{root}. Choose 'harvest' first to create one."
        )
    if len(matches) == 1:
        return os.path.join(root, matches[0])
    print("Multiple target files found:")
    for i, name in enumerate(matches, 1):
        print(f"  {i}. {name}")
    while True:
        choice = _ask(f"Choose a file [1-{len(matches)}]: ")
        if choice.isdigit() and 1 <= int(choice) <= len(matches):
            return os.path.join(root, matches[int(choice) - 1])
        print("Invalid choice.")


def run_action(verb, args):
    targets_file = find_targets_file()
    targets = read_targets(targets_file)

    print(f"\n{len(targets)} users to {verb} from {os.path.basename(targets_file)}"
          + (" (dry run)" if args.dry_run else "") + ".\n")

    if args.dry_run:
        for username in targets[:20]:
            print(f"  would {verb} {username}")
        if len(targets) > 20:
            print(f"  ... and {len(targets) - 20} more")
        print(f"\nDry run: {len(targets)} users would be {verb}ed (no requests sent).")
        return 0

    session = ghclient.make_session(ghclient.get_token())
    op = follow if verb == "follow" else unfollow

    # Route retry/quota notices through the bar so they don't corrupt it.
    previous_log = ghclient.log
    ghclient.log = tqdm.write
    counts = {"ok": 0, "error": 0}
    interrupted = False
    try:
        start = ghclient.get_rate_limit(session)
        if start:
            print(start)
        for username in tqdm(targets, unit="user"):
            ok, detail = op(session, username)
            counts["ok" if ok else "error"] += 1
            if not ok:
                tqdm.write(f"  {username}: error {detail}")
            time.sleep(random.uniform(args.min_delay, args.max_delay))
    except KeyboardInterrupt:
        interrupted = True
        print("\nInterrupted — stopping early.", file=sys.stderr)
    finally:
        ghclient.log = previous_log

    processed = counts["ok"] + counts["error"]
    status = "Stopped" if interrupted else "Done"
    past = "followed" if verb == "follow" else "unfollowed"
    print(f"\n{status}. {past}={counts['ok']} errors={counts['error']} "
          f"(processed {processed}/{len(targets)})")
    end = ghclient.rate_limit_summary()
    if end:
        print(end)
    return 130 if interrupted else 0


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-o", "--out",
        help="harvest output file for a single list "
             "(default: <user>_<followers|following>.txt; ignored when harvesting both)")
    parser.add_argument(
        "--fresh", action="store_true",
        help="for harvest, ignore any saved progress and start the list over")
    parser.add_argument(
        "--min-delay", type=float, default=1.0,
        help="minimum seconds to wait between follow/unfollow requests (default: %(default)s)")
    parser.add_argument(
        "--max-delay", type=float, default=2.0,
        help="maximum seconds to wait between follow/unfollow requests (default: %(default)s)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="for follow/unfollow, show what would happen without sending requests")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    choice = prompt_choice()
    if choice == "harvest":
        return harvest(args)
    return run_action(choice, args)


if __name__ == "__main__":
    sys.exit(main())
