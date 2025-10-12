#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dynamic Spotify playlist refresher with novelty enforcement.

Requires a refresh token minted with these scopes:
  playlist-modify-private
  playlist-modify-public
  playlist-read-private
  user-top-read
  user-read-recently-played
  user-library-read

ENV VARS (override as needed):
  SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REFRESH_TOKEN
  PLAYLIST_ID                (target playlist)
  COUNTRY_MARKET             (e.g., "IN" or "US")
  TIMEZONE                   (IANA tz; used only for logs, e.g., "Asia/Kolkata")
  N_TRACKS                   (default 50)
  TEMPO_MIN, TEMPO_MAX       (default 105, 132)
  ENERGY_MIN, ENERGY_MAX     (default 0.65, 0.85)
  FAMILIAR_RATIO             (default 0.70)
  CARRY_FRACTION             (default 0.20)
  MAX_REPEAT_FRACTION        (default 0.10)  # novelty across runs
  SEEN_WINDOW_DAYS           (default 30)    # novelty window
  STATE_PATH                 (default "state/seen.json")
"""

import os
import sys
import math
import time
import json
import random
import logging
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone

# third-party
import spotipy
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOauthError

# ----------------- CONFIG / ENV -----------------
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN", "")

PLAYLIST_ID = os.getenv("PLAYLIST_ID", "").strip()
MARKET = os.getenv("COUNTRY_MARKET", None) or None
TZ = os.getenv("TIMEZONE", "UTC")

N_TRACKS = int(os.getenv("N_TRACKS", "50"))
TEMPO_MIN = float(os.getenv("TEMPO_MIN", "105"))
TEMPO_MAX = float(os.getenv("TEMPO_MAX", "132"))
ENERGY_MIN = float(os.getenv("ENERGY_MIN", "0.65"))
ENERGY_MAX = float(os.getenv("ENERGY_MAX", "0.85"))
FAMILIAR_RATIO = float(os.getenv("FAMILIAR_RATIO", "0.70"))
CARRY_FRACTION = float(os.getenv("CARRY_FRACTION", "0.20"))
MAX_REPEAT_FRACTION = float(os.getenv("MAX_REPEAT_FRACTION", "0.10"))

STATE_PATH = Path(os.getenv("STATE_PATH", "state/seen.json"))
SEEN_WINDOW_DAYS = int(os.getenv("SEEN_WINDOW_DAYS", "30"))

# logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("refresh")

# ----------------- UTILITIES -----------------
def now_local():
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TZ)
        return datetime.now(tz)
    except Exception:
        return datetime.now(timezone.utc)

def uniq(seq):
    seen = set()
    out = []
    for x in seq:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out

def chunked(items, n):
    for i in range(0, len(items), n):
        yield items[i:i+n]

def safe_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        log.warning(f"[WARN] {getattr(fn, '__name__', str(fn))} failed: {e}")
        return None

# ----------------- AUTH -----------------
def get_access_token():
    if not (CLIENT_ID and CLIENT_SECRET and REFRESH_TOKEN):
        raise RuntimeError("Missing CLIENT_ID/CLIENT_SECRET/REFRESH_TOKEN")
    resp = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN},
        auth=(CLIENT_ID, CLIENT_SECRET),
        timeout=20,
    )
    if resp.status_code != 200:
        raise SpotifyOauthError(f"Token refresh failed: {resp.status_code} {resp.text}")
    return resp.json()["access_token"]

def sp_client():
    token = get_access_token()
    return Spotify(auth=token, requests_timeout=20, retries=3)

# ----------------- SEEN MEMORY (NOVELTY) -----------------
def load_seen():
    if STATE_PATH.exists():
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {"runs": []}
    cutoff_ts = (datetime.utcnow() - timedelta(days=SEEN_WINDOW_DAYS)).timestamp()
    data["runs"] = [r for r in data["runs"] if r.get("ts", 0) >= cutoff_ts]
    return data

def save_seen(data):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)

def seen_set(data):
    s = set()
    for r in data.get("runs", []):
        for tid in r.get("track_ids", []):
            s.add(tid)
    return s

def enforce_novelty(candidates, already_seen, max_repeat_frac, n_need):
    """Return up to n_need candidates with <= max_repeat_frac repeats."""
    max_repeats = math.floor(n_need * max_repeat_frac)
    fresh = [t for t in candidates if t not in already_seen]
    repeats = [t for t in candidates if t in already_seen]
    keep = []
    # fill mostly with fresh
    need = n_need
    take_fresh = min(len(fresh), max(0, need - max_repeats))
    keep.extend(fresh[:take_fresh])
    need -= take_fresh
    # allow limited repeats
    if need > 0 and max_repeats > 0:
        keep.extend(repeats[: min(need, max_repeats)])
    return keep[:n_need]

# ----------------- PLAYLIST IO -----------------
def read_playlist_track_ids(sp: Spotify, playlist_id: str):
    ids = []
    offset = 0
    while True:
        r = safe_call(sp.playlist_items, playlist_id, fields="items.track.id,total,next", limit=100, offset=offset, market=MARKET)
        if not r: break
        for it in r.get("items", []):
            tr = it.get("track") or {}
            tid = tr.get("id")
            if tid: ids.append(tid)
        if not r.get("next"):
            break
        offset += 100
    return ids

def write_playlist(sp: Spotify, playlist_id: str, track_ids):
    """Force a real refresh: clear then add (so Date added updates)."""
    current = read_playlist_track_ids(sp, playlist_id)
    if current:
        # remove all occurrences in chunks
        for batch in chunked(current, 100):
            uris = [f"spotify:track:{t}" for t in batch]
            safe_call(sp.playlist_remove_all_occurrences_of_items, playlist_id, uris)
            time.sleep(0.2)
    # then add back
    uris_final = [f"spotify:track:{t}" for t in track_ids]
    for batch in chunked(uris_final, 100):
        safe_call(sp.playlist_add_items, playlist_id, batch)
        time.sleep(0.2)

# ----------------- AUDIO FILTERING -----------------
def safe_audio_features(sp: Spotify, ids):
    out = []
    for batch in chunked(ids, 50):
        r = safe_call(sp.audio_features, batch)
        if r:
            out.extend([x for x in r if x])
    return out

def audio_filter(sp: Spotify, ids, tempo_min, tempo_max, energy_min, energy_max):
    """Keep ids whose audio features are within the given windows."""
    feats = safe_audio_features(sp, ids)
    ok = set()
    for f in feats:
        tid = f.get("id")
        if not tid: continue
        tempo = f.get("tempo")
        energy = f.get("energy")
        if tempo is None or energy is None: 
            continue
        if tempo_min <= float(tempo) <= tempo_max and energy_min <= float(energy) <= energy_max:
            ok.add(tid)
    return [i for i in ids if i in ok]

# ----------------- DISCOVERY POOL -----------------
def top_artists(sp: Spotify):
    out = []
    for tr in ["short_term", "medium_term"]:
        r = safe_call(sp.current_user_top_artists, limit=20, time_range=tr) or {}
        out += [a.get("id") for a in r.get("items", []) if a.get("id")]
    return uniq(out)

def related_artists(sp: Spotify, artist_ids, limit_each=10):
    out = []
    for aid in artist_ids:
        r = safe_call(sp.artist_related_artists, aid)
        if not r: 
            continue
        out += [a.get("id") for a in r.get("artists", []) if a.get("id")]
        if len(out) >= limit_each * len(artist_ids):
            break
    return uniq(out)

def recs(sp: Spotify, seed_art=None, seed_trk=None, limit=50, widen=0, prof=None):
    params = {
        "limit": limit,
        # seeds — Spotify requires at least one; prefer artists if available
    }
    if seed_art: params["seed_artists"] = ",".join(seed_art[:5])
    if seed_trk: params["seed_tracks"] = ",".join(seed_trk[:5])
    # target ranges
    if prof:
        e_min = max(0.0, prof["energy"][0] - widen)
        e_max = min(1.0, prof["energy"][1] + widen)
        t_min = max(0.0, prof["tempo"][0] - 2*widen*100)  # widen modestly
        t_max = prof["tempo"][1] + 2*widen*100
        params.update({
            "min_energy": e_min,
            "max_energy": e_max,
            "target_energy": (prof["energy"][0]+prof["energy"][1])/2.0,
            "min_tempo": t_min,
            "max_tempo": t_max,
            "target_tempo": (prof["tempo"][0]+prof["tempo"][1])/2.0,
        })
    r = safe_call(sp.recommendations, **params) or {}
    return [t.get("id") for t in r.get("tracks", []) if t and t.get("id")]

def search_tracks(sp: Spotify, queries, limit_each=25):
    out = []
    for q in queries:
        r = safe_call(sp.search, q=q, type="track", limit=limit_each, market=MARKET) or {}
        items = ((r.get("tracks") or {}).get("items")) or []
        out += [t.get("id") for t in items if t and t.get("id")]
    return uniq(out)

def build_discovery(sp: Spotify, prof, market, avoid_ids=set()):
    """
    Build a diversified discovery pool:
      - recs from top artists
      - recs from related artists
      - themed search (Bollywood/EDM/high energy) as extra source
      - audio features filter for tempo/energy window
    """
    pool = []

    # seeds from user's taste
    arts = top_artists(sp)
    if arts:
        pool += recs(sp, seed_art=arts[:5], limit=50, widen=0.0, prof=prof)
        rel = related_artists(sp, arts[:5], limit_each=10)
        if rel:
            pool += recs(sp, seed_art=rel[:5], limit=50, widen=0.05, prof=prof)

    # thematic searches to broaden novelty
    theme_q = [
        'genre:"bollywood" year:1970-2025',
        'genre:"edm" year:2010-2025',
        'tag:hipster',     # often less mainstream
        'remaster',
    ]
    pool += search_tracks(sp, theme_q, limit_each=25)

    # filter by audio window
    pool = uniq([t for t in pool if t not in avoid_ids])
    pool = audio_filter(sp, pool, prof["tempo"][0], prof["tempo"][1], prof["energy"][0], prof["energy"][1])
    random.shuffle(pool)
    return pool

# ----------------- BACKFILL -----------------
def backfill_to_size(sp, prof, pool, avoid, n_needed):
    got = list(pool)
    tries = 0
    while len(got) < n_needed and tries < 6:
        tries += 1
        widen = 0.03 * tries
        extra = recs(sp, seed_art=None, seed_trk=None, limit=50, widen=widen, prof=prof)
        extra = [t for t in extra if t not in avoid and t not in got]
        # filter by audio features (quick check)
        extra = audio_filter(sp, extra, prof["tempo"][0], prof["tempo"][1], prof["energy"][0], prof["energy"][1])
        got.extend(extra)
    return got[:n_needed]

# ----------------- PROFILE -----------------
def current_profile():
    return {
        "n_tracks": N_TRACKS,
        "tempo": (TEMPO_MIN, TEMPO_MAX),
        "energy": (ENERGY_MIN, ENERGY_MAX),
        "familiar_ratio": FAMILIAR_RATIO,
    }

# ----------------- MAIN -----------------
def main():
    if not PLAYLIST_ID:
        log.error("PLAYLIST_ID is required")
        return 2

    sp = sp_client()
    prof = current_profile()
    n_total = prof["n_tracks"]

    # novelty memory
    memory = load_seen()
    already_seen = seen_set(memory)

    # carry-over from current playlist
    current = read_playlist_track_ids(sp, PLAYLIST_ID)
    carry_n = int(math.floor(n_total * CARRY_FRACTION))
    carry = current[:carry_n] if current else []

    # familiar set (from top, saved, recent) then pick up to target
    familiar_target = int(math.floor(n_total * prof["familiar_ratio"]))
    familiar_ids = []

    # top tracks
    ts = safe_call(sp.current_user_top_tracks, limit=50, time_range="short_term") or {}
    tm = safe_call(sp.current_user_top_tracks, limit=50, time_range="medium_term") or {}
    familiar_ids += [t.get("id") for t in ts.get("items", []) if t.get("id")]
    familiar_ids += [t.get("id") for t in tm.get("items", []) if t.get("id")]

    # saved tracks
    saved = safe_call(sp.current_user_saved_tracks, limit=50, offset=0, market=MARKET) or {}
    familiar_ids += [i["track"]["id"] for i in saved.get("items", []) if i.get("track") and i["track"].get("id")]

    # recently played
    recent = safe_call(sp.current_user_recently_played, limit=50) or {}
    familiar_ids += [i["track"]["id"] for i in recent.get("items", []) if i.get("track") and i["track"].get("id")]

    familiar_ids = uniq([t for t in familiar_ids if t])
    familiar_pick = [t for t in familiar_ids if t not in carry][:familiar_target]

    # discovery
    remaining = max(0, n_total - len(carry) - len(familiar_pick))
    avoid = set(carry) | set(familiar_pick)
    disc_pool = build_discovery(sp, prof, MARKET, avoid_ids=avoid)
    if len(disc_pool) < remaining:
        disc_pool = disc_pool + backfill_to_size(sp, prof, disc_pool, avoid, remaining)

    # novelty gate (≤10% repeats across runs)
    novelty_needed = remaining
    novelty_candidates = [t for t in disc_pool if t not in avoid]
    novelty_pick = enforce_novelty(novelty_candidates, already_seen, MAX_REPEAT_FRACTION, novelty_needed)

    # final merge & pad if still short
    final_ids = uniq(carry + familiar_pick + novelty_pick)
    if len(final_ids) < n_total:
        # last resort pad: allow repeats from disc_pool but keep uniq
        for t in disc_pool:
            if len(final_ids) >= n_total:
                break
            if t not in final_ids:
                final_ids.append(t)

    final_ids = final_ids[:n_total]

    # write (clear → add) to force Date added updates
    write_playlist(sp, PLAYLIST_ID, final_ids)

    # update memory
    memory["runs"].append({"ts": datetime.utcnow().timestamp(), "track_ids": final_ids})
    save_seen(memory)

    log.info(f"OK: wrote {len(final_ids)} tracks to {PLAYLIST_ID} at {now_local().isoformat()}. "
             f"Window={{'tempo': ({TEMPO_MIN},{TEMPO_MAX}), 'energy': ({ENERGY_MIN},{ENERGY_MAX}), 'familiar_ratio': {FAMILIAR_RATIO}}}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
