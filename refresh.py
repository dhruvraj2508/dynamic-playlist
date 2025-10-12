#!/usr/bin/env python3
# refresh.py — robust Spotify playlist refresher with novelty + safe seeds
# - No genre seeds (avoid 404s)
# - No calls to endpoints that need extra scopes unless available
# - Always normalize seeds to bare IDs
# - Novelty memory via state/seen.json (commit this file in your repo)
# - Replaces playlist items, then tops-up if needed
# - Prints a compact report at the end

import os
import re
import sys
import json
import time
import math
import base64
import datetime as dt
from typing import List, Dict, Any, Optional, Iterable

import requests
import spotipy
from spotipy.client import Spotify, SpotifyException

# ------------------ Env & Defaults ------------------

ENV = os.environ.get

CLIENT_ID        = ENV("SPOTIFY_CLIENT_ID", "")
CLIENT_SECRET    = ENV("SPOTIFY_CLIENT_SECRET", "")
REFRESH_TOKEN    = ENV("SPOTIFY_REFRESH_TOKEN", "")
PLAYLIST_ID_RAW  = ENV("PLAYLIST_ID", "")
MARKET           = ENV("COUNTRY_MARKET", "IN")
TZ_NAME          = ENV("TIMEZONE", "Asia/Kolkata")

# Tuning & profile defaults (override via env if you want)
N_TRACKS         = int(ENV("N_TRACKS", "50"))
TEMPO_MIN        = float(ENV("TEMPO_MIN", "105"))
TEMPO_MAX        = float(ENV("TEMPO_MAX", "132"))
ENERGY_MIN       = float(ENV("ENERGY_MIN", "0.65"))
ENERGY_MAX       = float(ENV("ENERGY_MAX", "0.85"))
FAMILIAR_RATIO   = float(ENV("FAMILIAR_RATIO", "0.30"))
CARRY_FRACTION   = float(ENV("CARRY_FRACTION", "0.20"))

# Novelty memory
STATE_PATH       = ENV("STATE_PATH", "state/seen.json")
SEEN_WINDOW_DAYS = int(ENV("SEEN_WINDOW_DAYS", "30"))
MAX_REPEAT_FRAC  = float(ENV("MAX_REPEAT_FRACTION", "0.05"))

# Safety caps
MAX_WRITE_CHUNK  = 100
AUDIO_FEATURES_CHUNK = 40  # if you enable audio-features again later

# ------------------ Time helpers ------------------

def now_ist_str() -> str:
    # naive and simple: print local time string relative to Asia/Kolkata
    # (GitHub Actions machines are UTC; message is informational only)
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(TZ_NAME)
        return dt.datetime.now(tz).isoformat()
    except Exception:
        return dt.datetime.utcnow().isoformat() + "Z"

# ------------------ ID normalization ------------------

ID_RE = re.compile(r'([A-Za-z0-9]{22})')

def to_id(x: Optional[str]) -> Optional[str]:
    """Return the 22-char Spotify ID from an ID/URI/URL; else None."""
    if not x:
        return None
    m = ID_RE.search(x)
    return m.group(1) if m else None

def to_ids(xs: Iterable[str], limit: Optional[int] = None) -> List[str]:
    out = []
    for x in xs or []:
        xid = to_id(x)
        if xid:
            out.append(xid)
    if limit is not None:
        return out[:limit]
    return out

PLAYLIST_ID = to_id(PLAYLIST_ID_RAW) or PLAYLIST_ID_RAW

# ------------------ Auth with refresh token ------------------

def get_access_token_from_refresh() -> str:
    token_url = "https://accounts.spotify.com/api/token"
    auth_b64 = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    data = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
    }
    headers = {
        "Authorization": f"Basic {auth_b64}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    r = requests.post(token_url, data=data, headers=headers, timeout=20)
    r.raise_for_status()
    js = r.json()
    return js["access_token"]

class SPWrap:
    """Tiny wrapper that refreshes token on 401 and retries once."""
    def __init__(self):
        self.access_token = get_access_token_from_refresh()
        self.sp = Spotify(auth=self.access_token)

    def _refresh(self):
        self.access_token = get_access_token_from_refresh()
        self.sp = Spotify(auth=self.access_token)

    def call(self, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except SpotifyException as e:
            # 401 -> refresh and retry once
            if e.http_status == 401:
                self._refresh()
                return fn(*args, **kwargs)
            raise

# ------------------ Spotify helpers ------------------

def playlist_track_ids(sp: SPWrap, playlist_id: str) -> List[str]:
    ids = []
    offset = 0
    while True:
        res = sp.call(
            sp.sp.playlist_items,
            playlist_id,
            fields="items(track(id,uri)),total,next",
            additional_types=("track",),
            limit=100,
            offset=offset
        )
        for it in res.get("items", []):
            tr = it.get("track") or {}
            tid = tr.get("id")
            if tid:
                ids.append(tid)
        if res.get("next"):
            offset += 100
        else:
            break
    return ids

def user_top_tracks(sp: SPWrap, time_range: str) -> List[str]:
    try:
        res = sp.call(sp.sp.current_user_top_tracks, limit=50, time_range=time_range)
        return [t["id"] for t in res.get("items", []) if t and t.get("id")]
    except Exception as e:
        print(f"[WARN] top_tracks({time_range}) failed: {e}")
        return []

def playlist_replace_all(sp: SPWrap, playlist_id: str, uris: List[str]):
    # Replace first 100, then add the rest in chunks
    first = uris[:MAX_WRITE_CHUNK]
    sp.call(sp.sp.playlist_replace_items, playlist_id, first)
    rest = uris[MAX_WRITE_CHUNK:]
    for i in range(0, len(rest), MAX_WRITE_CHUNK):
        sp.call(sp.sp.playlist_add_items, playlist_id, rest[i:i+MAX_WRITE_CHUNK])

def playlist_count(sp: SPWrap, playlist_id: str) -> int:
    try:
        meta = sp.call(sp.sp.playlist_items, playlist_id, fields="total", limit=1)
        return int(meta.get("total", 0))
    except Exception:
        return 0

# ------------------ Recommendations (seed-safe) ------------------

def safe_recommendations(
    sp: SPWrap,
    *,
    seed_tracks: Optional[List[str]] = None,
    seed_artists: Optional[List[str]] = None,
    min_energy: Optional[float] = None,
    max_energy: Optional[float] = None,
    target_energy: Optional[float] = None,
    min_tempo: Optional[float] = None,
    max_tempo: Optional[float] = None,
    target_tempo: Optional[float] = None,
    limit: int = 50,
) -> List[str]:
    st = to_ids(seed_tracks or [], limit=5)
    sa = to_ids(seed_artists or [], limit=5)

    params: Dict[str, Any] = {"limit": limit}
    if st:
        params["seed_tracks"] = ",".join(st)
    elif sa:
        params["seed_artists"] = ",".join(sa)
    else:
        # No seeds: do not call this endpoint
        return []

    # Only add constraint keys that have values
    if min_energy is not None:  params["min_energy"] = min_energy
    if max_energy is not None:  params["max_energy"] = max_energy
    if target_energy is not None: params["target_energy"] = target_energy
    if min_tempo is not None:   params["min_tempo"] = min_tempo
    if max_tempo is not None:   params["max_tempo"] = max_tempo
    if target_tempo is not None: params["target_tempo"] = target_tempo

    try:
        rec = sp.call(sp.sp.recommendations, **params)
        return [t["id"] for t in rec.get("tracks", []) if t and t.get("id")]
    except Exception as e:
        print(f"[WARN] recommendations failed: {e}")
        return []

# ------------------ Novelty memory ------------------

def load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {"runs": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"runs": []}

def save_state(path: str, state: Dict[str, Any]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def prune_state(state: Dict[str, Any], days: int) -> Dict[str, Any]:
    if "runs" not in state:
        state["runs"] = []
        return state
    cutoff = dt.datetime.utcnow() - dt.timedelta(days=days)
    kept = []
    for run in state["runs"]:
        ts = run.get("ts")
        try:
            when = dt.datetime.fromisoformat(ts.replace("Z","")) if ts else None
        except Exception:
            when = None
        if when and when > cutoff:
            kept.append(run)
    state["runs"] = kept
    return state

def seen_recent_set(state: Dict[str, Any]) -> set:
    s = set()
    for run in state.get("runs", []):
        for tid in run.get("tracks", []):
            if tid:
                s.add(tid)
    return s

def record_run(state: Dict[str, Any], track_ids: List[str]):
    state.setdefault("runs", [])
    state["runs"].append({
        "ts": dt.datetime.utcnow().isoformat() + "Z",
        "tracks": list(track_ids),
        "n": len(track_ids),
    })

# ------------------ Profile (time-of-day window) ------------------

def current_profile() -> Dict[str, Any]:
    # You can tweak these windows later; they’re what you asked earlier:
    # 10–13 high-energy; 13–16 mellow; 16–20 focus high-energy; otherwise default.
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(TZ_NAME)
        now = dt.datetime.now(tz)
        hour = now.hour
    except Exception:
        hour = dt.datetime.utcnow().hour

    if 10 <= hour < 13:
        tempo = (105, 132)
        energy = (0.65, 0.85)
        fam = 0.70
    elif 13 <= hour < 16:
        tempo = (85, 110)
        energy = (0.40, 0.65)
        fam = 0.60
    elif 16 <= hour < 20:
        tempo = (100, 130)
        energy = (0.60, 0.85)
        fam = 0.70
    else:
        tempo = (TEMPO_MIN, TEMPO_MAX)
        energy = (ENERGY_MIN, ENERGY_MAX)
        fam = FAMILIAR_RATIO

    return {
        "n_tracks": N_TRACKS,
        "tempo": tempo,
        "energy": energy,
        "familiar_ratio": fam,
    }

# ------------------ Assembling the new playlist ------------------

def uniq(seq: Iterable[str]) -> List[str]:
    s = set()
    out = []
    for x in seq:
        if x and x not in s:
            s.add(x)
            out.append(x)
    return out

def clamp_novelty(candidates: List[str], seen_recent: set, n_total: int, max_repeat_frac: float) -> List[str]:
    """Ensure repeats across recent runs don't exceed max_repeat_frac of n_total."""
    max_repeats_allowed = int(math.floor(n_total * max_repeat_frac))
    fresh = [t for t in candidates if t not in seen_recent]
    repeats = [t for t in candidates if t in seen_recent]
    # allow only some repeats
    repeats = repeats[:max_repeats_allowed]
    return uniq(fresh + repeats)

def build_discovery(sp: SPWrap, seeds_tracks: List[str], seeds_artists: List[str], prof: Dict[str, Any], needed: int) -> List[str]:
    # Prefer track seeds; if empty, use artist seeds
    target_energy = (prof["energy"][0] + prof["energy"][1]) / 2.0
    target_tempo  = (prof["tempo"][0] + prof["tempo"][1]) / 2.0

    pool = []
    if seeds_tracks:
        pool += safe_recommendations(
            sp,
            seed_tracks=seeds_tracks[:5],
            min_energy=prof["energy"][0], max_energy=prof["energy"][1], target_energy=target_energy,
            min_tempo=prof["tempo"][0],  max_tempo=prof["tempo"][1],  target_tempo=target_tempo,
            limit=min(50, max(needed, 20)),
        )
    if not pool and seeds_artists:
        pool += safe_recommendations(
            sp,
            seed_artists=seeds_artists[:5],
            min_energy=prof["energy"][0], max_energy=prof["energy"][1], target_energy=target_energy,
            min_tempo=prof["tempo"][0],  max_tempo=prof["tempo"][1],  target_tempo=target_tempo,
            limit=min(50, max(needed, 20)),
        )

    return uniq(pool)

# ------------------ Main ------------------

def main() -> int:
    if not (CLIENT_ID and CLIENT_SECRET and REFRESH_TOKEN and PLAYLIST_ID):
        print("Missing credentials: ensure SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REFRESH_TOKEN, PLAYLIST_ID are set.")
        return 2

    sp = SPWrap()
    prof = current_profile()
    n_total = int(prof["n_tracks"])

    # 1) Read current playlist and compute carry
    current = playlist_track_ids(sp, PLAYLIST_ID)
    carry_n = int(math.floor(n_total * CARRY_FRACTION))
    carry = current[:carry_n] if current else []

    # 2) Familiar from user's top tracks (short + medium). If scopes missing, this just returns [] and we handle it.
    top_short = user_top_tracks(sp, "short_term")
    top_med   = user_top_tracks(sp, "medium_term")
    familiar_ids = uniq(top_short + top_med)
    familiar_target = int(math.floor(n_total * prof["familiar_ratio"]))
    familiar_pick = [t for t in familiar_ids if t not in carry][:familiar_target]

    # 3) Discovery from recommendations with safe seeds (playlist tracks first, then tops)
    remaining = max(0, n_total - len(carry) - len(familiar_pick))
    seed_from_playlist = current[:3] if current else []
    seed_from_top = familiar_ids[:3]
    discovery_pool = build_discovery(
        sp,
        seeds_tracks=seed_from_playlist or seed_from_top,
        seeds_artists=[],  # keep simple; we avoided artist fetches to reduce errors
        prof=prof,
        needed=remaining
    )
    discovery_ids = [d for d in discovery_pool if d not in carry and d not in familiar_pick][:remaining]

    # 4) Merge
    base_final = uniq(carry + familiar_pick + discovery_ids)[:n_total]

    # 5) Novelty memory across runs
    state = load_state(STATE_PATH)
    state = prune_state(state, SEEN_WINDOW_DAYS)
    recent_seen = seen_recent_set(state)
    final_ids = clamp_novelty(base_final, recent_seen, n_total, MAX_REPEAT_FRAC)

    # If novelty clamp reduced too much, try to backfill with more discovery
    if len(final_ids) < n_total:
        need = n_total - len(final_ids)
        more = build_discovery(
            sp,
            seeds_tracks=final_ids[:3] or seed_from_playlist or seed_from_top,
            seeds_artists=[],
            prof=prof,
            needed=need
        )
        fill = [t for t in more if t not in final_ids and t not in recent_seen][:need]
        final_ids = uniq(final_ids + fill)[:n_total]

    # 6) Write to playlist (replace + top-up guard)
    final_uris = [f"spotify:track:{tid}" for tid in final_ids]
    playlist_replace_all(sp, PLAYLIST_ID, final_uris)

    # Verify and top up if Spotify wrote fewer items (rare but happens)
    count_after = playlist_count(sp, PLAYLIST_ID)
    if count_after < n_total:
        need = n_total - count_after
        extra = safe_recommendations(
            sp,
            seed_tracks=final_ids[:3] or seed_from_playlist or seed_from_top,
            limit=min(50, max(20, need)),
            min_energy=prof["energy"][0], max_energy=prof["energy"][1],
            min_tempo=prof["tempo"][0],  max_tempo=prof["tempo"][1],
        )
        extra = [t for t in extra if t not in final_ids][:need]
        if extra:
            extra_uris = [f"spotify:track:{tid}" for tid in extra]
            for i in range(0, len(extra_uris), MAX_WRITE_CHUNK):
                sp.call(sp.sp.playlist_add_items, PLAYLIST_ID, extra_uris[i:i+MAX_WRITE_CHUNK])
            final_ids = uniq(final_ids + extra)[:n_total]

    # 7) Record novelty state and save
    record_run(state, final_ids)
    save_state(STATE_PATH, state)

    print(
        f"OK: wrote {len(final_ids)} tracks to *** at {now_ist_str()}."
        f" Window={{'tempo': {prof['tempo']}, 'energy': {prof['energy']}, 'familiar_ratio': {prof['familiar_ratio']}}}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
