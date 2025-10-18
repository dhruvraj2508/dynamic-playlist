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
CARRY_RATIO = float(ENV("CARRY_RATIO", "0.10"))         # 10% of current playlist carried into next run
MAX_REPEAT_FRACTION = float(ENV("MAX_REPEAT_FRACTION", "0.10"))  # ≤10% of playlist may repeat per run
NOVELTY_LOG_PATH = ENV("NOVELTY_LOG_PATH", "novelty_log.json")   # file in repo tracking recent runs
NOVELTY_KEEP_DAYS = int(ENV("NOVELTY_KEEP_DAYS", "14"))          # lookback window for repeats


# Tuning & profile defaults (override via env if you want)
N_TRACKS         = ENV_INT("N_TRACKS", 50)
FAMILIAR_RATIO   = ENV_FLOAT("FAMILIAR_RATIO", 0.6)
TEMPO_MIN        = ENV_FLOAT("TEMPO_MIN", 105.0)
TEMPO_MAX        = ENV_FLOAT("TEMPO_MAX", 132.0)
ENERGY_MIN       = ENV_FLOAT("ENERGY_MIN", 0.65)
ENERGY_MAX       = ENV_FLOAT("ENERGY_MAX", 0.85)
CARRY_PERCENT    = ENV_FLOAT("CARRY_PERCENT", 0.20)
REPEAT_CAP       = ENV_FLOAT("REPEAT_CAP_PERCENT", 0.10)
NOVELTY_DAYS     = ENV_INT("NOVELTY_LOOKBACK_DAYS", 30)
CARRY_FRACTION = float(os.getenv("CARRY_FRACTION", "0.20"))

# Novelty memory
STATE_PATH       = ENV("STATE_PATH", "state/seen.json")
SEEN_WINDOW_DAYS = int(ENV("SEEN_WINDOW_DAYS", "30"))
MAX_REPEAT_FRAC  = float(ENV("MAX_REPEAT_FRACTION", "0.05"))

# Safety caps
MAX_WRITE_CHUNK  = 100
AUDIO_FEATURES_CHUNK = 40  # if you enable audio-features again later


def ENV(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def ENV_INT(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v)  # works if v is a proper int string
    except (TypeError, ValueError):
        return default

def ENV_FLOAT(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


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

import json, datetime, os
from datetime import timezone, timedelta

def _today_iso_tz(tz_str):
    # re-use your TIMEZONE if you already have a helper; otherwise keep this simple UTC date
    return datetime.datetime.now(timezone.utc).date().isoformat()

def load_novelty_log(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and "runs" in data and isinstance(data["runs"], list):
                return data
    except Exception:
        pass
    return {"runs": []}

def prune_novelty_log(log, keep_days=14):
    cutoff = datetime.datetime.now(timezone.utc) - timedelta(days=keep_days)
    kept = []
    for r in log["runs"]:
        try:
            ts = datetime.datetime.fromisoformat(r.get("ts"))
        except Exception:
            ts = None
        if ts is None or ts >= cutoff:
            kept.append(r)
    log["runs"] = kept
    return log

def save_novelty_log(path, log):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)

def recent_track_ids(log):
    ids = set()
    for r in log.get("runs", []):
        for tid in r.get("tracks", []):
            if tid:
                ids.add(tid)
    return ids

def cap_repeats(prev_ids, desired_ids, n_total, max_repeat_fraction):
    """
    Ensure at most floor(n_total * max_repeat_fraction) items overlap with prev_ids.
    We keep items in order and drop overflow; caller can backfill with new picks.
    """
    max_repeats = max(0, int(n_total * max_repeat_fraction))
    out = []
    repeats_kept = 0
    for tid in desired_ids:
        if tid in prev_ids:
            if repeats_kept < max_repeats:
                out.append(tid)
                repeats_kept += 1
            else:
                # skip this repeated one; caller should later fill the gap with novel picks
                continue
        else:
            out.append(tid)
    return out, max_repeats, repeats_kept


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
        fam = 0.60
    elif 13 <= hour < 16:
        tempo = (85, 110)
        energy = (0.40, 0.65)
        fam = 0.60
    elif 16 <= hour < 20:
        tempo = (100, 130)
        energy = (0.60, 0.85)
        fam = 0.60
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

# ---------- Main ----------
def main():
    sp = sp_client()
    prof = current_profile()

    # force the 60/40 split you asked for (override whatever profile says)
    prof["familiar_ratio"] = 0.60
    n_total = int(prof.get("n_tracks", 50))

    # 1) Carry over up to 20% from the current playlist (keeps a little continuity)
    current_ids = read_playlist_track_ids(sp, PLAYLIST_ID) or []
    carry_n = max(0, int(math.floor(n_total * 0.20)))
    carry = current_ids[:carry_n]

    # 2) Build the familiar pool from your Top Tracks (short + medium term)
    top_short = safe_call(sp.current_user_top_tracks, limit=50, time_range="short_term") or {}
    top_med   = safe_call(sp.current_user_top_tracks, limit=50, time_range="medium_term") or {}
    top_short_items = top_short.get("items", []) if isinstance(top_short, dict) else []
    top_med_items   = top_med.get("items", []) if isinstance(top_med, dict) else []
    familiar_pool = uniq([t.get("id") for t in (top_short_items + top_med_items) if t and t.get("id")])

    # 3) Pick familiar (excluding anything we already carry)
    familiar_target = max(0, int(math.floor(n_total * prof["familiar_ratio"])))
    familiar_pick = [t for t in familiar_pool if t not in carry][:familiar_target]

    # 4) Discovery pool (exclude carry + familiar + the rest of current to avoid “re-adds”)
    avoid = set(carry) | set(familiar_pick) | set(current_ids)
    discovery_pool = build_discovery(sp, prof, MARKET, avoid_ids=avoid) or []

    # 5) Fill discovery slice
    remaining = max(0, n_total - len(carry) - len(familiar_pick))
    discovery_ids = [d for d in discovery_pool if d not in avoid][:remaining]

    # 6) If discovery ran short, top up so we ALWAYS hit n_total
    if len(discovery_ids) < remaining:
        # first, try to top up with more familiar (still excluding already chosen)
        need = remaining - len(discovery_ids)
        familiar_topup = [t for t in familiar_pool
                          if t not in carry and t not in familiar_pick and t not in discovery_ids][:need]
        discovery_ids.extend(familiar_topup)

    if len(carry) + len(familiar_pick) + len(discovery_ids) < n_total:
        # absolute last resort: take from current playlist leftovers (keeps length stable)
        need = n_total - (len(carry) + len(familiar_pick) + len(discovery_ids))
        leftovers = [t for t in current_ids if t not in carry and t not in familiar_pick and t not in discovery_ids][:need]
        discovery_ids.extend(leftovers)

    # 7) Final assembly (dedup + truncate to target)
    final_ids = uniq(carry + familiar_pick + discovery_ids)[:n_total]
    final_uris = [f"spotify:track:{tid}" for tid in final_ids]

    # 8) Write back to the SAME playlist (handle >100 in safe chunks)
    # Replace first batch (up to 100)
    first_batch = final_uris[:100]
    safe_call(sp.playlist_replace_items, PLAYLIST_ID, first_batch)

    # Append any remaining in 100-sized chunks
    idx = 100
    while idx < len(final_uris):
        safe_call(sp.playlist_add_items, PLAYLIST_ID, final_uris[idx:idx+100])
        idx += 100

    # 9) Log a clear summary so you can verify splits & lengths in the Actions logs
    print(
        "OK: wrote {n} tracks to {pl} at {ts}. "
        "Breakdown: carry={c}, familiar={f}, discovery={d}. "
        "Window={{'n_tracks': {n_total}, 'tempo': {tempo}, 'energy': {energy}, 'familiar_ratio': {fr}}}".format(
            n=len(final_uris),
            pl=PLAYLIST_ID,
            ts=now_ist(),
            c=len(carry),
            f=len(familiar_pick),
            d=len([x for x in final_ids if x not in set(carry) | set(familiar_pick)]),
            n_total=n_total,
            tempo=prof.get("tempo"),
            energy=prof.get("energy"),
            fr=prof["familiar_ratio"],
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
