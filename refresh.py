#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dynamic Spotify playlist refresher with:
- 60/40 familiar/discovery
- novelty tracked across runs (persisted in repo: state/seen.json, state/history.csv)
- robust logging + markdown/JSON report + NDJSON event stream under reports/<ts>/
- resilient recs with widening and graceful fallbacks
- zero reliance on audio-features (to avoid 403 spikes)
"""

import os, sys, json, math, time, random, datetime, pathlib, csv, traceback
from typing import List, Dict, Any, Optional, Set, Tuple

# -------------------------------
# Environment & Config
# -------------------------------

def env_str(k: str, default: str = "") -> str:
    v = os.getenv(k)
    return v if v is not None and v != "" else default

def env_int(k: str, default: int) -> int:
    v = os.getenv(k)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v)
    except Exception:
        return default

def env_float(k: str, default: float) -> float:
    v = os.getenv(k)
    if v is None or v.strip() == "":
        return default
    try:
        return float(v)
    except Exception:
        return default

SPOTIFY_CLIENT_ID     = env_str("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = env_str("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REFRESH_TOKEN = env_str("SPOTIFY_REFRESH_TOKEN")

PLAYLIST_ID           = env_str("PLAYLIST_ID")  # target playlist to replace
MARKET                = env_str("COUNTRY_MARKET", "US")

# Window knobs (safe defaults)
N_TRACKS        = env_int("N_TRACKS", 50)
FAMILIAR_RATIO  = env_float("FAMILIAR_RATIO", 0.60)  # familiar 60%, discovery 40%
CARRY_FRACTION  = env_float("CARRY_FRACTION", 0.20)  # carry 20% of current playlist order
MIN_TEMPO       = env_float("MIN_TEMPO", 105.0)
MAX_TEMPO       = env_float("MAX_TEMPO", 132.0)
TARGET_TEMPO    = (MIN_TEMPO + MAX_TEMPO) / 2.0
MIN_ENERGY      = env_float("MIN_ENERGY", 0.65)
MAX_ENERGY      = env_float("MAX_ENERGY", 0.85)
TARGET_ENERGY   = (MIN_ENERGY + MAX_ENERGY) / 2.0

# Novelty / seen memory
SEEN_MAX     = env_int("SEEN_MAX", 10000)        # cap the seen list to last 10k items (sliding)
NOVELTY_DAYS = env_int("NOVELTY_DAYS", 3650)     # consider anything ever-seen as "seen" (10y). Tune if you want decay.

# State & Reports dirs (committed back to repo by workflow)
STATE_DIR   = pathlib.Path("state")
REPORTS_DIR = pathlib.Path("reports")

STATE_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

RUN_TS = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
RUN_DIR = REPORTS_DIR / RUN_TS
RUN_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------
# Logging & run report
# -------------------------------

import logging
LOG_LEVEL = env_str("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("dp-refresh")

RUN: Dict[str, Any] = {
    "started_utc": datetime.datetime.utcnow().isoformat() + "Z",
    "env": {
        "PLAYLIST_ID": PLAYLIST_ID,
        "MARKET": MARKET,
        "N_TRACKS": N_TRACKS,
        "FAMILIAR_RATIO": FAMILIAR_RATIO,
        "CARRY_FRACTION": CARRY_FRACTION,
        "MIN_TEMPO": MIN_TEMPO,
        "MAX_TEMPO": MAX_TEMPO,
        "MIN_ENERGY": MIN_ENERGY,
        "MAX_ENERGY": MAX_ENERGY
    },
    "profile_window": {},
    "counts": {
        "carry": 0, "familiar": 0, "discovery": 0,
        "final": 0, "deduped": 0, "widen_attempts": 0
    },
    "exclusions": {
        "seen_excluded": 0,
        "already_in_playlist": 0
    },
    "api_warnings": [],
    "seeds": {"artists": [], "tracks": [], "genres": []},
    "debug_samples": {
        "carry": [], "familiar": [], "discovery_pool": [],
        "discovery_pick": [], "final": []
    },
    "final_track_ids": [],
}

def event(where: str, **kv):
    with (RUN_DIR / "events.ndjson").open("a", encoding="utf-8") as f:
        row = {"where": where, **kv}
        f.write(json.dumps(row) + "\n")

def warn_api(where: str, err: Exception):
    msg = f"{type(err).__name__}: {err}"
    RUN["api_warnings"].append({"where": where, "error": msg})
    log.warning("[%s] %s", where, msg)
    event(where, level="WARN", error=msg)

def write_reports():
    # JSON
    with (RUN_DIR / "report.json").open("w", encoding="utf-8") as f:
        json.dump(RUN, f, indent=2)

    # Markdown
    md = []
    md.append(f"# Dynamic Playlist Refresh — {RUN_TS}\n")
    md.append("## Window\n")
    md.append(f"- Tracks: `{N_TRACKS}`  \n- Familiar ratio: `{FAMILIAR_RATIO}`  \n- Carry: `{CARRY_FRACTION}`  \n")
    md.append(f"- Tempo: `({MIN_TEMPO},{MAX_TEMPO})`  \n- Energy: `({MIN_ENERGY},{MAX_ENERGY})`\n")
    md.append("\n## Counts\n")
    for k, v in RUN["counts"].items():
        md.append(f"- **{k}**: {v}")
    if RUN["api_warnings"]:
        md.append("\n## API Warnings\n")
        for w in RUN["api_warnings"]:
            md.append(f"- `{w['where']}` → {w['error']}")
    md.append("\n## Samples (IDs)\n")
    for k, arr in RUN["debug_samples"].items():
        md.append(f"- {k}: `{arr[:10]}`")
    (RUN_DIR / "report.md").write_text("\n".join(md), encoding="utf-8")

    # CSV of final tracks (id + source)
    final_csv = RUN_DIR / "final.csv"
    with final_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["track_id", "source_bucket"])
        for tid, src in RUN["_final_sources"]:
            w.writerow([tid, src])

    print(f"REPORT_DIR={RUN_DIR}")  # visible in logs for workflow to pick up

# -------------------------------
# Spotify client
# -------------------------------
# We use spotipy with refresh-token flow. No client creds or auth-code during the job.

import spotipy
from spotipy.oauth2 import SpotifyOAuth

def sp_client() -> spotipy.Spotify:
    scope = "playlist-read-private playlist-modify-private playlist-modify-public user-top-read user-library-read"
    auth = SpotifyOAuth(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
        redirect_uri="https://example.com/callback",
        scope=scope,
        cache_path=None
    )
    # monkey-patch token cache with refresh_token we already have
    auth.refresh_token = SPOTIFY_REFRESH_TOKEN
    token_info = auth.refresh_access_token(SPOTIFY_REFRESH_TOKEN)
    sp = spotipy.Spotify(auth=token_info["access_token"], requests_timeout=20, retries=3, status_forcelist=(429, 500, 502, 503, 504))
    return sp

# -------------------------------
# Helpers (IDs, pagers, unique)
# -------------------------------

def uniq(a: List[str]) -> List[str]:
    s, out = set(), []
    for x in a:
        if x and x not in s:
            s.add(x); out.append(x)
    return out

def track_ids_from_items(items: List[Dict[str, Any]]) -> List[str]:
    out = []
    for it in items:
        # Supports both track objects and playlist track wrappers
        track = it.get("track", it)
        if track and track.get("id"):
            out.append(track["id"])
    return out

def playlist_track_ids(sp: spotipy.Spotify, playlist_id: str, limit: int = 1000) -> List[str]:
    out = []
    offset = 0
    while True:
        try:
            page = sp.playlist_items(playlist_id, fields="items(track(id)),next", additional_types=("track",), limit=100, offset=offset)
        except Exception as e:
            warn_api("playlist_items", e); break
        items = page.get("items", []) or []
        out.extend(track_ids_from_items(items))
        if not page.get("next") or len(out) >= limit:
            break
        offset += 100
    return out

def current_user_top(sp: spotipy.Spotify, time_range: str) -> List[str]:
    try:
        res = sp.current_user_top_tracks(limit=50, time_range=time_range)
        return track_ids_from_items(res.get("items", []) or [])
    except Exception as e:
        warn_api(f"current_user_top_tracks[{time_range}]", e)
        return []

def current_user_top_artists(sp: spotipy.Spotify, time_range: str) -> List[str]:
    try:
        res = sp.current_user_top_artists(limit=50, time_range=time_range)
        ids = [a["id"] for a in (res.get("items", []) or []) if a and a.get("id")]
        return ids
    except Exception as e:
        warn_api(f"current_user_top_artists[{time_range}]", e)
        return []

def saved_tracks(sp: spotipy.Spotify, max_take: int = 200) -> List[str]:
    out, offset = [], 0
    # user-library-read scope required; we handle 403 gracefully
    while len(out) < max_take:
        try:
            page = sp.current_user_saved_tracks(limit=50, offset=offset)
        except Exception as e:
            warn_api("current_user_saved_tracks", e); break
        items = page.get("items", []) or []
        out.extend(track_ids_from_items(items))
        if len(items) < 50:
            break
        offset += 50
    return out[:max_take]

# -------------------------------
# State persistence (seen/history)
# -------------------------------

SEEN_PATH    = STATE_DIR / "seen.json"
HISTORY_PATH = STATE_DIR / "history.csv"

def load_seen() -> List[str]:
    if SEEN_PATH.exists():
        try:
            return json.loads(SEEN_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            warn_api("load_seen", e)
    return []

def save_seen(seen_ids: List[str]):
    # cap sliding window
    if len(seen_ids) > SEEN_MAX:
        seen_ids = seen_ids[-SEEN_MAX:]
    try:
        SEEN_PATH.write_text(json.dumps(seen_ids, indent=0), encoding="utf-8")
    except Exception as e:
        warn_api("save_seen", e)

def append_history(run_ts: str, final_ids: List[str], sources: List[Tuple[str, str]]):
    # sources is list of (track_id, bucket)
    try:
        newfile = not HISTORY_PATH.exists()
        with HISTORY_PATH.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if newfile:
                w.writerow(["run_ts", "ordinal", "track_id", "bucket"])
            for i, (tid, bucket) in enumerate(sources, 1):
                w.writerow([run_ts, i, tid, bucket])
    except Exception as e:
        warn_api("append_history", e)

# -------------------------------
# Discovery / Recommendations
# -------------------------------

def recs(
    sp: spotipy.Spotify,
    limit: int,
    seed_artists: List[str],
    seed_tracks: List[str],
    energy: Tuple[float,float],
    tempo: Tuple[float,float],
    market: str
) -> List[str]:
    """
    Get recommendations with widening if sparse.
    """
    min_e, max_e = energy
    min_t, max_t = tempo
    params_base = {
        "limit": min(100, max(1, limit)),
        "min_energy": max(0.0, min_e),
        "max_energy": min(1.0, max_e),
        "min_tempo": max(0.0, min_t),
        "max_tempo": max_t,
    }
    # seed up to 5 total (artists + tracks)
    seeds_a = seed_artists[:3]
    seeds_t = seed_tracks[:2]
    out: List[str] = []

    widen_steps = [
        {},  # exact window
        {"min_energy": max(0.0, min_e - 0.05), "max_energy": min(1.0, max_e + 0.05)},
        {"min_tempo": max(0.0, min_t - 6.0), "max_tempo": max_t + 6.0},
        {"min_energy": max(0.0, min_e - 0.10), "max_energy": min(1.0, max_e + 0.10),
         "min_tempo": max(0.0, min_t - 12.0), "max_tempo": max_t + 12.0},
        {"min_energy": 0.5, "max_energy": 1.0, "min_tempo": 90.0, "max_tempo": 160.0},
    ]

    tried = 0
    for bump in widen_steps:
        tried += 1
        RUN["counts"]["widen_attempts"] = tried
        params = dict(params_base)
        params.update(bump)
        if seeds_a:
            params["seed_artists"] = ",".join(seeds_a)
        if seeds_t:
            params["seed_tracks"] = ",".join(seeds_t)
        try:
            r = sp.recommendations(**params)
            items = r.get("tracks", []) or []
            ids = [t["id"] for t in items if t and t.get("id")]
            out.extend(ids)
            if len(out) >= limit:
                break
        except Exception as e:
            warn_api("recommendations", e)
            continue
    return uniq(out)[:limit]

def build_familiar(
    sp: spotipy.Spotify,
    carry_ids: List[str],
    target_n: int
) -> List[str]:
    # Top tracks (short + medium) + saved tracks first
    t_short = current_user_top(sp, "short_term")
    t_med   = current_user_top(sp, "medium_term")
    lib     = saved_tracks(sp, max_take=200)  # if scope available

    pool = uniq(t_short + t_med + lib)
    pool = [t for t in pool if t not in carry_ids]
    random.shuffle(pool)
    return pool[:target_n]

def build_discovery(
    sp: spotipy.Spotify,
    seed_artists: List[str],
    seed_tracks: List[str],
    avoid_ids: Set[str],
    target_n: int
) -> List[str]:
    # primary: recommendations from user seeds
    ids = recs(sp, target_n * 2, seed_artists, seed_tracks,
               energy=(MIN_ENERGY, MAX_ENERGY), tempo=(MIN_TEMPO, MAX_TEMPO),
               market=MARKET)
    ids = [i for i in ids if i not in avoid_ids]
    if len(ids) >= target_n:
        random.shuffle(ids)
        return ids[:target_n]

    # secondary: broaden with catalog fallbacks (bollywood/edm/pop)
    catalog_seeds = [
        ("seed_genres", "bollywood,edm,pop"),
        ("seed_genres", "indian-pop,edm,pop"),
    ]
    for k, v in catalog_seeds:
        try:
            r = sp.recommendations(
                limit=max(10, target_n),
                **{k: v},
                min_energy=MIN_ENERGY, max_energy=MAX_ENERGY,
                min_tempo=MIN_TEMPO, max_tempo=MAX_TEMPO,
            )
            items = r.get("tracks", []) or []
            extra = [t["id"] for t in items if t and t.get("id")]
            for x in extra:
                if x not in avoid_ids:
                    ids.append(x)
            ids = uniq(ids)
            if len(ids) >= target_n:
                break
        except Exception as e:
            warn_api("recommendations[catalog]", e)
            continue

    random.shuffle(ids)
    return ids[:target_n]

# -------------------------------
# Main
# -------------------------------

def main() -> int:
    print(f"Starting refresh at {datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}")
    sp = sp_client()

    # Window (exposed to report)
    RUN["profile_window"] = {
        "n_tracks": N_TRACKS,
        "tempo": (MIN_TEMPO, MAX_TEMPO),
        "energy": (MIN_ENERGY, MAX_ENERGY),
        "familiar_ratio": FAMILIAR_RATIO
    }

    # 1) Read current playlist + compute carry
    current_ids = playlist_track_ids(sp, PLAYLIST_ID) or []
    carry_n = max(0, min(N_TRACKS, int(math.floor(N_TRACKS * CARRY_FRACTION))))
    carry = current_ids[:carry_n]
    RUN["counts"]["carry"] = len(carry)
    RUN["debug_samples"]["carry"] = carry[:10]
    event("carry", count=len(carry))

    # 2) Familiar (60%)
    familiar_target = max(0, int(round(N_TRACKS * FAMILIAR_RATIO)))
    familiar_ids = build_familiar(sp, carry, familiar_target)
    RUN["counts"]["familiar"] = len(familiar_ids)
    RUN["debug_samples"]["familiar"] = familiar_ids[:10]
    event("familiar_pick", count=len(familiar_ids))

    # 3) Discovery (40%) – novelty enforced vs seen.json
    need = max(0, N_TRACKS - len(carry) - len(familiar_ids))
    # Seeds for discovery from user tastes
    top_art   = current_user_top_artists(sp, "short_term") + current_user_top_artists(sp, "medium_term")
    top_tracks= (current_ids[:20] or []) + current_user_top(sp, "short_term")[:20]
    top_art = uniq(top_art)
    top_tracks = uniq(top_tracks)
    RUN["seeds"]["artists"] = top_art[:10]
    RUN["seeds"]["tracks"]  = top_tracks[:10]

    avoid: Set[str] = set(carry) | set(familiar_ids)
    # Load seen memory
    seen_list = load_seen()
    seen: Set[str] = set(seen_list)
    discovery_pool = build_discovery(sp, top_art, top_tracks, avoid_ids=avoid | seen, target_n=max(need, 10))
    discovery_ids = discovery_pool[:need]
    # If still short, allow partial overlap with seen (very mild) to fill up
    if len(discovery_ids) < need:
        shortfall = need - len(discovery_ids)
        backfill = [t for t in build_discovery(sp, top_art, top_tracks, avoid_ids=avoid, target_n=shortfall*2)
                    if t not in avoid][:shortfall]
        discovery_ids.extend(backfill)

    RUN["counts"]["discovery"] = len(discovery_ids)
    RUN["debug_samples"]["discovery_pool"] = discovery_pool[:10]
    RUN["debug_samples"]["discovery_pick"] = discovery_ids[:10]
    event("discovery_pick", count=len(discovery_ids))

    # 4) Merge, dedupe, cap
    ordered = uniq(carry + familiar_ids + discovery_ids)[:N_TRACKS]
    RUN["counts"]["final"] = len(ordered)
    RUN["counts"]["deduped"] = (len(carry) + len(familiar_ids) + len(discovery_ids)) - len(ordered)
    RUN["debug_samples"]["final"] = ordered[:10]
    RUN["final_track_ids"] = ordered[:]
    # Source tags for CSV
    final_sources: List[Tuple[str, str]] = []
    s_carry = set(carry); s_fam = set(familiar_ids); s_dis = set(discovery_ids)
    for tid in ordered:
        if tid in s_carry:      final_sources.append((tid, "carry"))
        elif tid in s_fam:      final_sources.append((tid, "familiar"))
        elif tid in s_dis:      final_sources.append((tid, "discovery"))
        else:                   final_sources.append((tid, "other"))

    RUN["_final_sources"] = final_sources  # internal for CSV write

    # 5) Write playlist (replace)
    uris = [f"spotify:track:{tid}" for tid in ordered]
    try:
        sp.playlist_replace_items(PLAYLIST_ID, uris)
    except Exception as e:
        warn_api("playlist_replace_items", e)
        # Try slow path: clear + add in chunks
        try:
            sp.playlist_remove_all_occurrences_of_items(PLAYLIST_ID, [{"uri": u} for u in uris])
        except Exception as e2:
            warn_api("playlist_remove_all_occurrences_of_items", e2)
        # add back
        i = 0
        while i < len(uris):
            try:
                sp.playlist_add_items(PLAYLIST_ID, uris[i:i+100])
            except Exception as e3:
                warn_api("playlist_add_items", e3)
                break
            i += 100

    # 6) Persist memory (seen + history), then write reports
    # Update seen with everything we *attempted* to add this run
    new_seen = uniq(seen_list + ordered)
    save_seen(new_seen)
    append_history(RUN_TS, ordered, final_sources)

    print(f"OK: wrote {len(ordered)} tracks to {PLAYLIST_ID} at {datetime.datetime.utcnow().isoformat()}Z. "
          f"Window={{'tempo': ({MIN_TEMPO},{MAX_TEMPO}), 'energy': ({MIN_ENERGY},{MAX_ENERGY}), 'familiar_ratio': {FAMILIAR_RATIO}}}")

    return 0

if __name__ == "__main__":
    code = 1
    try:
        code = main()
    except Exception as e:
        warn_api("fatal", e)
        traceback.print_exc()
    finally:
        write_reports()
    sys.exit(code)

