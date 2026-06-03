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
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from tqdm import tqdm

import ghclient

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
TARGETS_PATTERN = re.compile(r".+_follow(?:ers|ing)\.txt$")

# Concurrent page fetches during harvest. GitHub's followers/following lists
# support numbered pagination, so pages can be pulled in parallel rather than
# one-at-a-time — the difference between latency-bound and rate-limit-bound.
HARVEST_WORKERS = 10


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


def _fetch_page(session, user, kind, page, on_retry=None):
    """Fetch one page (up to 100 logins) of `user`'s `kind` list by page number.

    Uses numbered pagination so pages are independent and fetchable in parallel.
    Idempotent GETs get a higher retry budget than the default.
    """
    response = ghclient.request(
        session, "GET", f"{ghclient.API_BASE}/users/{user}/{kind}",
        params={"per_page": 100, "page": page},
        max_retries=8, on_retry=on_retry,
    )
    response.raise_for_status()
    return [entry["login"] for entry in response.json()]


class _Counter:
    """A thread-safe callable that counts how many times it was invoked."""
    def __init__(self):
        self.count = 0
        self._lock = threading.Lock()

    def __call__(self):
        with self._lock:
            self.count += 1


def _count_lines(path):
    """Return how many lines `path` already has (0 if it doesn't exist)."""
    try:
        with open(path, "rb") as handle:
            return sum(1 for _ in handle)
    except FileNotFoundError:
        return 0


def _remove(path):
    """Remove a file if it exists."""
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def _harvest_one(session, user, kind, total, outfile, fresh=False, workers=HARVEST_WORKERS):
    """Harvest one list into `outfile`, fetching `workers` pages concurrently.

    The output file is its own checkpoint: each line is one username, so a re-run
    resumes from `<lines>/100 + 1`. Stopped runs (Ctrl-C, crash, or a page that
    fails every retry) just need to be re-run. `fresh=True` starts the list over.
    """
    _remove(f"{outfile}.state")  # legacy cursor file from the old sequential harvester
    if fresh:
        _remove(outfile)

    saved = _count_lines(outfile)
    total_pages = (total + 99) // 100 if total else None

    if total and saved >= total:
        print(f"\n{outfile} already has {saved} {kind} — nothing to do.")
        return 0
    if saved:
        print(f"\nResuming {user}'s {kind} from {saved} already saved → {outfile}...")
    else:
        print(f"\n{user} has {total} {kind}, fetching to {outfile} "
              f"({workers} workers)...")

    next_page = saved // 100 + 1
    retries = _Counter()
    error = None
    interrupted = False
    pool = ThreadPoolExecutor(max_workers=max(1, workers))
    try:
        with open(outfile, "a", encoding="utf-8") as handle, \
                tqdm(total=total, initial=saved, unit="user") as bar:
            done = False
            while not done:
                pages = [p for p in range(next_page, next_page + workers)
                         if total_pages is None or p <= total_pages]
                if not pages:
                    break
                # Submit the whole batch at once, then consume results in page
                # order so the file stays ordered and the resume point is exact.
                futures = {p: pool.submit(_fetch_page, session, user, kind, p, retries)
                           for p in pages}
                for p in pages:
                    try:
                        logins = futures[p].result()
                    except requests.exceptions.HTTPError as exc:
                        error = exc
                        break
                    if not logins:          # past the last page → finished
                        done = True
                        break
                    handle.write("".join(login + "\n" for login in logins))
                    saved += len(logins)
                    bar.update(len(logins))
                    next_page = p + 1
                handle.flush()
                if error:
                    break
    except KeyboardInterrupt:
        interrupted = True
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    if interrupted:
        print(f"\nStopped — re-run to resume {kind} from {saved} ({outfile}).",
              file=sys.stderr)
        return 130
    if error is not None:
        print(f"\nStopped after an API error ({error}); re-run to resume {kind} "
              f"from {saved} ({outfile}).", file=sys.stderr)
        return 1

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
                              outfile, fresh=args.fresh, workers=args.workers)
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
        "--workers", type=int, default=HARVEST_WORKERS,
        help="concurrent page fetches during harvest (default: %(default)s)")
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
