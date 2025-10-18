#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dynamic Spotify playlist refresher (carry + familiar/discovery with novelty guard)

What this script does
- Auth via Refresh Token (no interactive prompt)
- Reads the current playlist, carries a fraction forward
- Pulls your Top Tracks/Artists (if scope allowed), otherwise falls back
- Builds discovery via /recommendations with tempo/energy window
- Enforces approx 60/40 Familiar/Discovery (configurable)
- Avoids unsupported seeds and empty params (prevents 400s)
- Gracefully handles 403 scopes by skipping those calls (prevents crashes)
- Replaces items in the SAME playlist
- Writes a small run report (text) to the workspace (optional)

Environment (set as GitHub Actions “Secrets and variables” → “Variables”, unless secret):
- SPOTIFY_CLIENT_ID        (secret)
- SPOTIFY_CLIENT_SECRET    (secret)
- SPOTIFY_REFRESH_TOKEN    (secret)
- PLAYLIST_ID              e.g. 7w5J0EdQB2UOE6LrMZK0bq
- COUNTRY_MARKET           e.g. IN  (optional; default: None)
- TIMEZONE                 e.g. Asia/Kolkata (optional; default: UTC)
- N_TRACKS                 e.g. 50  (int; default: 50)
- CARRY_FRACTION           e.g. 0.20 (float; default: 0.20)
- FAMILIAR_RATIO           e.g. 0.60 (float; default: 0.60)  # discovery will be 1 - this
- TEMPO_MIN                e.g. 105 (float; default: 105)
- TEMPO_MAX                e.g. 132 (float; default: 132)
- ENERGY_MIN               e.g. 0.65 (float; default: 0.65)
- ENERGY_MAX               e.g. 0.85 (float; default: 0.85)

Requires:
  pip install spotipy==2.23.0 requests==2.*
"""

from __future__ import annotations
import os, sys, json, math, time, random, typing as T
from datetime import datetime, timezone
try:
    import zoneinfo
    tz_get = zoneinfo.ZoneInfo
except Exception:
    tz_get = None

import requests
import spotipy
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
from spotipy.client import Spotify
from spotipy.exceptions import SpotifyException

# ----------------------------
# Env helpers
# ----------------------------
def ENV(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name)
    if val is None or str(val).strip() == "":
        return default
    return val

def ENV_INT(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or str(val).strip() == "":
        return int(default)
    try:
        return int(str(val).strip())
    except Exception:
        return int(default)

def ENV_FLOAT(name: str, default: float) -> float:
    val = os.getenv(name)
    if val is None or str(val).strip() == "":
        return float(default)
    try:
        return float(str(val).strip())
    except Exception:
        return float(default)

# ----------------------------
# Config from ENV
# ----------------------------
CLIENT_ID      = ENV("SPOTIFY_CLIENT_ID")
CLIENT_SECRET  = ENV("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN  = ENV("SPOTIFY_REFRESH_TOKEN")
PLAYLIST_ID    = ENV("PLAYLIST_ID")
MARKET         = ENV("COUNTRY_MARKET", None)
TZ_NAME        = ENV("TIMEZONE", "UTC")

N_TRACKS       = ENV_INT("N_TRACKS", 50)
CARRY_FRACTION = ENV_FLOAT("CARRY_FRACTION", 0.20)     # keep only this; no carry percent var
FAMILIAR_RATIO = ENV_FLOAT("FAMILIAR_RATIO", 0.60)     # you asked for 60/40

TEMPO_MIN      = ENV_FLOAT("TEMPO_MIN", 105.0)
TEMPO_MAX      = ENV_FLOAT("TEMPO_MAX", 132.0)
ENERGY_MIN     = ENV_FLOAT("ENERGY_MIN", 0.65)
ENERGY_MAX     = ENV_FLOAT("ENERGY_MAX", 0.85)

# Safety clamps
FAMILIAR_RATIO = min(max(FAMILIAR_RATIO, 0.0), 1.0)
CARRY_FRACTION = min(max(CARRY_FRACTION, 0.0), 0.9)
if TEMPO_MIN > TEMPO_MAX: TEMPO_MIN, TEMPO_MAX = TEMPO_MAX, TEMPO_MIN
if ENERGY_MIN > ENERGY_MAX: ENERGY_MIN, ENERGY_MAX = ENERGY_MAX, ENERGY_MIN

# ----------------------------
# Small utils
# ----------------------------
def now_local_iso() -> str:
    if TZ_NAME and tz_get:
        tz = tz_get(TZ_NAME)
    else:
        tz = timezone.utc
    return datetime.now(tz).isoformat()

def uniq(seq: T.Iterable[T.Any]) -> list:
    """Stable unique preserving order, ignoring falsy ids."""
    seen = set()
    out = []
    for x in seq:
        if not x: 
            continue
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

def chunks(lst: list, n: int) -> T.Iterable[list]:
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def log_warn(msg: str):
    print(f"[WARN] {msg}", file=sys.stderr)

# ----------------------------
# Auth via Refresh Token (no browser)
# ----------------------------
def get_token_via_refresh(client_id: str, client_secret: str, refresh_token: str) -> dict:
    token_url = "https://accounts.spotify.com/api/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    auth = (client_id, client_secret)
    r = requests.post(token_url, data=data, auth=auth, timeout=30)
    r.raise_for_status()
    return r.json()

def sp_client() -> Spotify:
    if not (CLIENT_ID and CLIENT_SECRET and REFRESH_TOKEN):
        raise SystemExit("Missing CLIENT_ID/CLIENT_SECRET/REFRESH_TOKEN")

    try:
        token = get_token_via_refresh(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
        access_token = token.get("access_token")
        if not access_token:
            raise RuntimeError("Failed to obtain access token from refresh grant.")
        sp = spotipy.Spotify(auth=access_token)
        return sp
    except Exception as e:
        raise SystemExit(f"Auth failed: {e}")

# ----------------------------
# Spotify helpers (safe wrappers)
# ----------------------------
def safe_call(fn: T.Callable, *args, **kwargs):
    """Call Spotipy fn; return {} or [] on error and log a warning."""
    try:
        return fn(*args, **kwargs)
    except SpotifyException as e:
        log_warn(f"{fn.__name__} failed: http status: {e.http_status}, code: {e.code} - {e.msg}")
        return {} if isinstance({}, type({})) else []
    except requests.HTTPError as e:
        log_warn(f"{fn.__name__} failed: {e}")
        return {} if isinstance({}, type({})) else []
    except Exception as e:
        log_warn(f"{fn.__name__} failed: {e}")
        return {} if isinstance({}, type({})) else []

def current_user_id(sp: Spotify) -> str | None:
    me = safe_call(sp.me)
    if isinstance(me, dict):
        return me.get("id")
    return None

def playlist_track_ids(sp: Spotify, playlist_id: str, limit: int | None = None) -> list[str]:
    """Read track IDs from a playlist; returns IDs (not URIs)."""
    out = []
    offset = 0
    while True:
        resp = safe_call(sp.playlist_items, playlist_id, limit=100, offset=offset, additional_types=("track",))
        if not resp or not isinstance(resp, dict):
            break
        items = resp.get("items", [])
        for it in items:
            tr = (it or {}).get("track") or {}
            tid = tr.get("id")
            if tid:
                out.append(tid)
                if limit and len(out) >= limit:
                    return out[:limit]
        if resp.get("next"):
            offset += 100
        else:
            break
    return out

# ----------------------------
# Familiar pool (with graceful scope fallback)
# ----------------------------
def user_top_tracks(sp: Spotify, time_range: str = "medium_term", limit: int = 50) -> list[str]:
    """Return list of track IDs, handles missing scope by returning []."""
    try:
        resp = sp.current_user_top_tracks(limit=limit, time_range=time_range)
        items = (resp or {}).get("items", [])
        return [ (t or {}).get("id") for t in items if (t or {}).get("id") ]
    except SpotifyException as e:
        # 403 insufficient scope → fallback to []
        log_warn(f"current_user_top_tracks failed: http status: {e.http_status}, code: {e.code} - {e.msg}")
        return []
    except Exception as e:
        log_warn(f"current_user_top_tracks failed: {e}")
        return []

def user_top_artists(sp: Spotify, time_range: str = "medium_term", limit: int = 50) -> list[str]:
    """Return list of artist IDs, handles missing scope by returning []."""
    try:
        resp = sp.current_user_top_artists(limit=limit, time_range=time_range)
        items = (resp or {}).get("items", [])
        return [ (a or {}).get("id") for a in items if (a or {}).get("id") ]
    except SpotifyException as e:
        log_warn(f"current_user_top_artists failed: http status: {e.http_status}, code: {e.code} - {e.msg}")
        return []
    except Exception as e:
        log_warn(f"current_user_top_artists failed: {e}")
        return []

# ----------------------------
# Recommendations (discovery) — robust seed handling
# ----------------------------
def recommendations(
    sp: Spotify,
    *,
    seed_artists: list[str] | None,
    seed_tracks: list[str] | None,
    limit: int,
    tempo_min: float,
    tempo_max: float,
    energy_min: float,
    energy_max: float,
    market: str | None,
) -> list[str]:
    """
    Wraps sp.recommendations with careful seed selection and prevents empty/invalid params.
    Returns a list of track IDs.
    """
    seed_artists = [s for s in (seed_artists or []) if s]
    seed_tracks  = [s for s in (seed_tracks or []) if s]

    # Spotify allows up to 5 combined seeds. Use at least 1 valid seed when possible.
    seeds_used = False
    params: dict = {"limit": max(1, min(100, limit))}
    if seed_artists:
        params["seed_artists"] = seed_artists[: min(5, len(seed_artists))]
        seeds_used = True
    if seed_tracks:
        # If artists already used, fill remaining seed slots with tracks
        remaining = 5 - len(params.get("seed_artists", []))
        if remaining > 0:
            params["seed_tracks"] = seed_tracks[:remaining]
            seeds_used = True

    # If we somehow have no seeds (scope issues), we still call recommendations with target filters.
    # Some deployments/regions return 404 with only target params. We progressively relax if that happens.
    params["min_energy"]   = energy_min
    params["max_energy"]   = energy_max
    params["target_energy"]= (energy_min + energy_max) / 2.0
    params["min_tempo"]    = tempo_min
    params["max_tempo"]    = tempo_max
    params["target_tempo"] = (tempo_min + tempo_max) / 2.0
    if market:
        params["market"] = market

    # Try up to 5 passes, relaxing constraints if strictly no seeds and we hit 404
    out_ids: list[str] = []
    for attempt in range(5):
        try:
            rec = sp.recommendations(**params)
            tracks = (rec or {}).get("tracks", []) or []
            out_ids = [ (t or {}).get("id") for t in tracks if (t or {}).get("id") ]
            if out_ids:
                break
            # Otherwise, slightly broaden
            params["min_energy"] = max(0.0, params["min_energy"] - 0.03)
            params["max_energy"] = min(1.0, params["max_energy"] + 0.03)
            params["min_tempo"]  = max(50.0, params["min_tempo"] - 6.0)
            params["max_tempo"]  = min(220.0, params["max_tempo"] + 6.0)
        except SpotifyException as e:
            log_warn(f"recommendations failed: http status: {e.http_status}, code: {e.code} - {e.msg}")
            # Relax on 404 only; otherwise just break
            if e.http_status == 404:
                params["min_energy"] = max(0.0, params["min_energy"] - 0.05)
                params["max_energy"] = min(1.0, params["max_energy"] + 0.05)
                params["min_tempo"]  = max(40.0, params["min_tempo"] - 12.0)
                params["max_tempo"]  = min(240.0, params["max_tempo"] + 12.0)
                continue
            else:
                break
        except Exception as e:
            log_warn(f"recommendations unexpected error: {e}")
            break

    return uniq(out_ids)

# ----------------------------
# Novelty store (optional lightweight)
# ----------------------------
NOVELTY_FILE = "novelty_history.json"
def novelty_load() -> set[str]:
    try:
        with open(NOVELTY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set([x for x in data if isinstance(x, str)])
        return set()
    except Exception:
        return set()

def novelty_save(ids: list[str], max_keep: int = 2000) -> None:
    prev = []
    try:
        with open(NOVELTY_FILE, "r", encoding="utf-8") as f:
            prev = json.load(f)
            if not isinstance(prev, list):
                prev = []
    except Exception:
        prev = []
    merged = uniq(prev + ids)
    if len(merged) > max_keep:
        merged = merged[-max_keep:]
    try:
        with open(NOVELTY_FILE, "w", encoding="utf-8") as f:
            json.dump(merged, f)
    except Exception:
        pass

# ----------------------------
# Main flow
# ----------------------------
def main() -> int:
    print(f"Starting refresh at {datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()}Z")

    sp = sp_client()
    if not PLAYLIST_ID:
        raise SystemExit("PLAYLIST_ID is required.")

    # Read current playlist and compute carry
    current_ids = playlist_track_ids(sp, PLAYLIST_ID) or []
    carry_n = int(math.floor(N_TRACKS * CARRY_FRACTION))
    carry_ids = current_ids[:carry_n] if carry_n > 0 else []

    # Familiar pool (top tracks if possible, else fallback to current)
    top_short = user_top_tracks(sp, "short_term", 50)
    top_med   = user_top_tracks(sp, "medium_term", 50)
    familiar_pool = uniq(top_short + top_med)
    if not familiar_pool:
        # Fallback: use whatever is in the playlist as "familiar"
        familiar_pool = uniq(current_ids)

    # Familiar pick (avoid duplicating carry)
    familiar_target = int(round(N_TRACKS * FAMILIAR_RATIO))
    familiar_pick = [t for t in familiar_pool if t not in carry_ids][:familiar_target]

    # Seeds for discovery: prefer user top artists/tracks (if present), else use familiar pool
    seed_art = user_top_artists(sp, "medium_term", 50)
    if not seed_art:
        # derive artists from familiar tracks metadata (cheaply via tracks endpoint)
        # If this fails (rate/403), we’ll just leave seed_art empty
        try:
            meta = []
            for batch in chunks(familiar_pool[:50], 50):
                resp = sp.tracks(batch, market=MARKET) if MARKET else sp.tracks(batch)
                meta.extend((resp or {}).get("tracks", []) or [])
            seed_art = uniq([ ((t or {}).get("artists") or [{}])[0].get("id") for t in meta if t ])
        except Exception as e:
            log_warn(f"seed artist derivation failed: {e}")
            seed_art = []

    seed_trk = familiar_pool[:50]

    # Discovery pool via /recommendations (no genres to avoid 404s you were seeing)
    discovery_needed = max(0, N_TRACKS - len(carry_ids) - len(familiar_pick))
    discovery_pool: list[str] = []
    if discovery_needed > 0:
        # Try a few passes with shuffled seeds to get variety
        rand_art = seed_art[:]
        rand_trk = seed_trk[:]
        random.shuffle(rand_art)
        random.shuffle(rand_trk)

        tries = max(3, min(6, (discovery_needed // 10) + 3))
        for i in range(tries):
            use_art = rand_art[i*3:(i+1)*3]
            use_trk = rand_trk[i*2:(i+1)*2]
            rec_ids = recommendations(
                sp,
                seed_artists=use_art,
                seed_tracks=use_trk,
                limit=min(50, max(10, discovery_needed*2)),
                tempo_min=TEMPO_MIN, tempo_max=TEMPO_MAX,
                energy_min=ENERGY_MIN, energy_max=ENERGY_MAX,
                market=MARKET
            )
            discovery_pool.extend(rec_ids)
            if len(uniq(discovery_pool)) >= discovery_needed * 2:
                break

    # Novelty: avoid repeats across runs for discovery portion only (not familiar/carry)
    novelty_seen = novelty_load()
    discovery_pool = [d for d in uniq(discovery_pool) if d not in novelty_seen and d not in carry_ids and d not in familiar_pick]
    discovery_pick = discovery_pool[:discovery_needed]

    # If discovery too small, backfill with more familiar (but still avoiding carry) or random recs without seeds
    if len(discovery_pick) < discovery_needed:
        deficit = discovery_needed - len(discovery_pick)
        backfill = [t for t in familiar_pool if t not in carry_ids and t not in familiar_pick and t not in discovery_pick]
        discovery_pick += backfill[:deficit]

    # Final stitch
    final_ids = uniq(carry_ids + familiar_pick + discovery_pick)[:N_TRACKS]
    final_uris = [f"spotify:track:{tid}" for tid in final_ids]

    # Write to the SAME playlist
    try:
        sp.playlist_replace_items(PLAYLIST_ID, final_uris[:100])
        # If N_TRACKS > 100, append remaining
        remaining = final_uris[100:]
        pos = 0
        while remaining:
            batch = remaining[:100]
            remaining = remaining[100:]
            sp.playlist_add_items(PLAYLIST_ID, batch, position=pos+len(final_uris[:100])+pos)
            pos += len(batch)
    except SpotifyException as e:
        raise SystemExit(f"Failed to write playlist: http status {e.http_status} code {e.code} - {e.msg}")

    # Save novelty history for future runs (only discovery portion)
    novelty_save(discovery_pick)

    # Lightweight report
    window = {
        "tempo": (TEMPO_MIN, TEMPO_MAX),
        "energy": (ENERGY_MIN, ENERGY_MAX),
        "familiar_ratio": FAMILIAR_RATIO,
        "carry_fraction": CARRY_FRACTION,
        "market": MARKET,
        "n_tracks": N_TRACKS,
    }
    print(f"OK: wrote {len(final_uris)} tracks to *** at {now_local_iso()}. Window={window}")

    # Optional: write a tiny text file so you can open it from Actions artifacts/logs
    try:
        with open("run_report.txt", "w", encoding="utf-8") as f:
            f.write(f"Run at {now_local_iso()}\n")
            f.write(f"Final count: {len(final_uris)}\n")
            f.write(json.dumps(window, indent=2))
            f.write("\nFirst 10 IDs:\n")
            f.write("\n".join(final_ids[:10]))
            f.write("\n")
    except Exception:
        pass

    return 0

# ----------------------------
# Entry
# ----------------------------
if __name__ == "__main__":
    raise SystemExit(main())
