"""Shared GitHub REST API helpers for the follow bot.

Centralizes token loading, the authenticated session, and a single
rate-limit- and retry-aware request wrapper used across `main.py`.
"""
import os
import random
import sys
import time

import requests
from dotenv import load_dotenv

API_BASE = "https://api.github.com"
GRAPHQL_URL = f"{API_BASE}/graphql"
API_VERSION = "2022-11-28"

# Connect / read timeout (seconds) applied to every request.
TIMEOUT = (5, 30)
MAX_RETRIES = 5

# Warn once when the primary rate limit drops below this many requests.
LOW_QUOTA_THRESHOLD = 100

# Latest primary rate-limit snapshot, refreshed from every response.
_rate = {"remaining": None, "limit": None, "reset": None}
_low_warned = False

# Where retry/quota notices go. Callers (e.g. a tqdm progress bar) can redirect
# this to `tqdm.write` so messages don't corrupt the bar.
log = lambda message: print(message, file=sys.stderr)  # noqa: E731

# Values that appear when someone copies .env.example without editing it.
_PLACEHOLDERS = {"", "your_token_here", "ghp_xxxxyyyzzz1234567890abcdefg"}


def get_token(env_file=".env"):
    """Return GITHUB_TOKEN, loading `env_file` first if it exists.

    Exits with a clear message when the token is missing or still a placeholder.
    """
    load_dotenv(env_file)
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token in _PLACEHOLDERS or "xxxx" in token.lower():
        sys.exit(
            "GITHUB_TOKEN is not set. Copy .env.example to .env and add a real "
            "token, or export GITHUB_TOKEN. See the README for required scopes."
        )
    return token


def make_session(token):
    """Return a requests.Session pre-configured for the GitHub REST API."""
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": API_VERSION,
    })
    return session


def _jitter(seconds):
    """Spread retries out a little so many requests don't wake in lockstep."""
    return seconds * (1 + random.random() * 0.5)


def _sleep(seconds, reason):
    """Log a wait and sleep — for rare, meaningful waits (rate limits)."""
    log(f"\n  {reason}; waiting {int(seconds)}s before retrying...")
    time.sleep(seconds)


def _retry_sleep(seconds, reason, on_retry):
    """Sleep for a transient retry: count it via `on_retry`, or log if no counter."""
    if on_retry is not None:
        on_retry()
    else:
        log(f"\n  {reason}; waiting {seconds:.0f}s before retrying...")
    time.sleep(seconds)


def _retry_after_seconds(response):
    """Seconds to wait per the response's rate-limit headers, or None."""
    retry_after = response.headers.get("Retry-After")
    if retry_after is not None:
        try:
            return max(0, int(retry_after))
        except ValueError:
            return None
    if response.headers.get("X-RateLimit-Remaining") == "0":
        reset = response.headers.get("X-RateLimit-Reset")
        if reset is not None:
            try:
                return max(0, int(reset) - int(time.time()))
            except ValueError:
                return None
    return None


def _is_secondary_limit(response):
    """True if the body looks like a secondary/abuse rate-limit message."""
    try:
        message = response.json().get("message", "")
    except ValueError:
        message = response.text
    message = message.lower()
    return "secondary rate limit" in message or "abuse" in message


def _reset_minutes():
    """Whole minutes until the primary rate limit resets, or None."""
    reset = _rate["reset"]
    if reset is None:
        return None
    try:
        return max(0, (int(reset) - int(time.time())) // 60)
    except (TypeError, ValueError):
        return None


def _note_rate_limit(response):
    """Record the primary rate-limit headers and warn once when running low."""
    global _low_warned
    remaining = response.headers.get("X-RateLimit-Remaining")
    if remaining is None:
        return
    try:
        remaining = int(remaining)
    except ValueError:
        return
    _rate["remaining"] = remaining
    _rate["limit"] = response.headers.get("X-RateLimit-Limit")
    _rate["reset"] = response.headers.get("X-RateLimit-Reset")
    if remaining < LOW_QUOTA_THRESHOLD and not _low_warned:
        mins = _reset_minutes()
        when = f" (resets in ~{mins}m)" if mins is not None else ""
        log(f"\n  ⚠ low API quota: {remaining} requests left{when}")
        _low_warned = True
    elif remaining >= LOW_QUOTA_THRESHOLD:
        _low_warned = False  # quota refreshed — re-arm the warning


def rate_limit_summary():
    """A printable one-liner for the current quota, or '' if unknown."""
    if _rate["remaining"] is None:
        return ""
    limit = _rate["limit"] or "?"
    mins = _reset_minutes()
    when = f" (resets in ~{mins}m)" if mins is not None else ""
    return f"API quota: {_rate['remaining']}/{limit} left{when}"


def request(session, method, url, *, max_retries=MAX_RETRIES, on_retry=None, **kwargs):
    """Issue a request, transparently retrying transient failures.

    Handles GitHub's primary and secondary rate limits (sleeping for the
    server-advised interval), 5xx responses, and network errors with capped,
    jittered exponential backoff. Returns the final ``requests.Response``; the
    caller decides how to treat the 2xx/4xx status code.

    `on_retry`, if given, is called once per transient (5xx/network) retry
    instead of logging it — handy for collapsing thousands of retries into a
    single end-of-run summary. Rate-limit waits are always logged.
    """
    kwargs.setdefault("timeout", TIMEOUT)
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            response = session.request(method, url, **kwargs)
        except requests.exceptions.RequestException as exc:
            if attempt == max_retries:
                raise
            _retry_sleep(_jitter(backoff), f"network error ({exc.__class__.__name__})", on_retry)
            backoff = min(backoff * 2, 60)
            continue

        _note_rate_limit(response)

        if response.status_code in (403, 429):
            wait = _retry_after_seconds(response)
            if wait is None and _is_secondary_limit(response):
                wait, backoff = backoff, min(backoff * 2, 60)
            if wait is not None and attempt < max_retries:
                _sleep(wait + 1, "rate limited by GitHub")
                continue

        if response.status_code >= 500 and attempt < max_retries:
            _retry_sleep(_jitter(backoff), f"server error {response.status_code}", on_retry)
            backoff = min(backoff * 2, 60)
            continue

        return response

    return response


class GraphQLError(RuntimeError):
    """A non-retryable GraphQL error (bad query, NOT_FOUND, auth, ...)."""


def graphql(session, query, variables=None, *, max_retries=MAX_RETRIES, on_retry=None):
    """POST a GraphQL query and return its ``data`` dict.

    Transport failures (network, 5xx, HTTP 429) are retried by ``request``. A
    GraphQL ``RATE_LIMITED`` error arrives as an HTTP 200 with an ``errors``
    entry, so it's handled here: wait for the reset and retry. Any other GraphQL
    error raises ``GraphQLError``.
    """
    payload = {"query": query, "variables": variables or {}}
    for attempt in range(1, max_retries + 1):
        response = request(session, "POST", GRAPHQL_URL, json=payload,
                           max_retries=max_retries, on_retry=on_retry)
        response.raise_for_status()
        try:
            body = response.json()
        except ValueError:
            raise GraphQLError("GraphQL response was not JSON")

        errors = body.get("errors")
        if errors:
            if (any(e.get("type") == "RATE_LIMITED" for e in errors)
                    and attempt < max_retries):
                wait = _retry_after_seconds(response)
                if wait is None:
                    reset = _rate.get("reset")
                    wait = max(0, int(reset) - int(time.time())) if reset else 60
                _sleep(wait + 1, "GraphQL rate limited")
                continue
            raise GraphQLError("; ".join(e.get("message", str(e)) for e in errors))

        data = body.get("data")
        if data is None:
            raise GraphQLError("GraphQL response contained no data")
        return data

    raise GraphQLError("GraphQL request still rate-limited after retries")


def get_rate_limit(session):
    """Read the current quota from the free `/rate_limit` endpoint and return a summary.

    `GET /rate_limit` is exempt from the primary rate limit, so this costs nothing.
    """
    response = request(session, "GET", f"{API_BASE}/rate_limit")
    try:
        core = response.json()["resources"]["core"]
        _rate["remaining"] = core["remaining"]
        _rate["limit"] = core["limit"]
        _rate["reset"] = core["reset"]
    except (ValueError, KeyError, TypeError):
        pass
    return rate_limit_summary()
