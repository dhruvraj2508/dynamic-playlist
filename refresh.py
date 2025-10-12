#!/usr/bin/env python3
# refresh.py

import os
import time
import math
import json
import logging
import datetime as dt
from typing import List, Iterable, Dict, Any, Tuple, Set

import requests
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.client import Spotify
from spotipy.exceptions import SpotifyException

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

# ----------------- Config via ENV -----------------
PLAYLIST_ID    = os.environ.get("PLAYLIST_ID", "").strip()
MARKET         = os.environ.get("COUNTRY_MARKET", "IN").strip() or "IN"
TZNAME         = os.environ.get("TIMEZONE", "Asia/Kolkata").strip() or "Asia/Kolkata"

CLIENT_ID      = os.environ["SPOTIFY_CLIENT_ID"]
CLIENT_SECRET  = os.environ["SPOTIFY_CLIENT_SECRET"]
REFRESH_TOKEN  = os.environ["SPOTIFY_REFRESH_TOKEN"]

# Novelty/testing knobs (env-driven; safe defaults)
CARRY_OVER_PCT     = float(os.environ.get("CARRY_OVER_PCT", "0.0"))   # set 0.0 while testing
FAMILIAR_RATIO_OVR = os.environ.get("FAMILIAR_RATIO", "")             # e.g. "0.5" to override profile
NOVEL_WINDOW_DAYS  = int(os.environ.get("NOVEL_WINDOW_DAYS", "21"))   # history lookback for repeats
MAX_REPEAT_PCT     = float(os.environ.get("MAX_REPEAT_PCT", "0.10"))  # ≤10% repeats across runs
NOVELTY_MIN_DIFF   = float(os.environ.get("NOVELTY_MIN_DIFF", "0.60"))# require ≥60% new vs last run
WRITE_REPORT       = os.environ.get("WRITE_REPORT", "1") == "1"

# history constraints
HISTORY_PATH   = "playlist_history.json"
HISTORY_DAYS   = NOVEL_WINDOW_DAYS  # use the env-driven lookback

# Logging: hush spotipy
logging.getLogger("spotipy").setLevel(logging.ERROR)

# -------------- General Helpers -------------------
def now_ist() -> str:
    try:
        if ZoneInfo:
            tz = ZoneInfo(TZNAME)
            return dt.datetime.now(tz).isoformat()
    except Exception:
        pass
    return dt.datetime.now().isoformat()

def uniq(seq: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for x in seq:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def chunked(seq: Iterable[Any], n: int) -> Iterable[List[Any]]:
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) == n:
            yield buf
            buf = []
    if buf:
        yield buf

def safe_call(fn, *args, **kwargs):
    """Return {} on exception for dict-like endpoints to avoid crashing."""
    try:
        return fn(*args, **kwargs) or {}
    except Exception:
        return {}

# -------------- OAuth Clients ---------------------
def sp_user_client() -> Spotify:
    """User-scoped client: mints a fresh access token each run from long-lived refresh token."""
    resp = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN},
        auth=(CLIENT_ID, CLIENT_SECRET),
        timeout=20,
    )
    resp.raise_for_status()
    access_token = resp.json()["access_token"]
    return spotipy.Spotify(auth=access_token)

def sp_app_client() -> Spotify:
    """App client (Client Credentials) for non-user endpoints, reduces rate-limits on user token."""
    mgr = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    return spotipy.Spotify(auth_manager=mgr)

# -------------- Profile (Time-of-day) -------------
def current_profile() -> Dict[str, Any]:
    """
    Windows (local timezone):
      - 10:00–13:00 high-energy work: tempo 105–132, energy 0.65–0.85, familiar 0.70
      - 13:00–16:00 mellow:           tempo  85–110, energy 0.35–0.60, familiar 0.60
      - 16:00–20:00 focus/high:       tempo 105–132, energy 0.65–0.85, familiar 0.70
      - otherwise balanced default.
    """
    try:
        tz = ZoneInfo(TZNAME) if ZoneInfo else None
    except Exception:
        tz = None
    now = dt.datetime.now(tz) if tz else dt.datetime.now()
    h = now.hour

    if 10 <= h < 13:
        return {"n_tracks": 50, "tempo": (105, 132), "energy": (0.65, 0.85), "familiar_ratio": 0.70}
    if 13 <= h < 16:
        return {"n_tracks": 45, "tempo": (85, 110), "energy": (0.35, 0.60), "familiar_ratio": 0.60}
    if 16 <= h < 20:
        return {"n_tracks": 50, "tempo": (105, 132), "energy": (0.65, 0.85), "familiar_ratio": 0.70}
    # default
    return {"n_tracks": 50, "tempo": (96, 126), "energy": (0.50, 0.80), "familiar_ratio": 0.65}

# -------------- Read/Write Playlist ---------------
def read_playlist_track_ids(sp_user: Spotify, playlist_id: str) -> List[str]:
    ids = []
    results = safe_call(sp_user.playlist_items, playlist_id, additional_types=("track",))
    while results and results.get("items"):
        for it in results["items"]:
            tr = it.get("track") or {}
            tid = tr.get("id")
            if tid:
                ids.append(tid)
        if results.get("next"):
            try:
                results = sp_user.next(results)
            except Exception:
                break
        else:
            break
    return ids

# -------------- User Libraries (novelty guards) ---
def get_recent_track_ids(sp_user: Spotify, limit=50) -> List[str]:
    try:
        res = sp_user.current_user_recently_played(limit=limit)
        return [it["track"]["id"] for it in res.get("items", []) if it.get("track") and it["track"].get("id")]
    except Exception:
        return []

def get_saved_track_ids(sp_user: Spotify, max_items=300) -> List[str]:
    ids, got = [], 0
    try:
        while got < max_items:
            page = sp_user.current_user_saved_tracks(limit=50, offset=got, market=None)
            items = page.get("items", [])
            if not items:
                break
            ids.extend([it["track"]["id"] for it in items if it.get("track") and it["track"].get("id")])
            got += len(items)
            if not page.get("next"):
                break
    except Exception:
        pass
    return ids

def recent_playlist_ids(sp_user: Spotify, playlist_id: str, hours=96) -> List[str]:
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours)
    ids = []
    try:
        results = sp_user.playlist_items(playlist_id, additional_types=("track",))
    except Exception:
        return ids
    while True:
        for it in results.get("items", []):
            added_at = it.get("added_at")
            if not added_at:
                continue
            try:
                when = dt.datetime.fromisoformat(added_at.replace("Z", "+00:00"))
            except Exception:
                continue
            if when >= cutoff:
                tr = it.get("track") or {}
                tid = tr.get("id")
                if tid:
                    ids.append(tid)
        if results.get("next"):
            try:
                results = sp_user.next(results)
            except Exception:
                break
        else:
            break
    return ids

# -------------- Track Meta (for reports) ----------
def fetch_track_meta(sp_app: Spotify, ids: List[str]) -> Dict[str, Dict[str, str]]:
    meta = {}
    for chunk in chunked(ids, 50):
        try:
            res = sp_app.tracks(chunk)
            for t in (res.get("tracks") or []):
                if not t:
                    continue
                tid = t.get("id")
                if not tid:
                    continue
                name = t.get("name") or ""
                arts = ", ".join(a.get("name", "") for a in (t.get("artists") or []))
                meta[tid] = {"name": name, "artists": arts}
        except Exception:
            continue
    return meta

# -------------- Discovery Builders ----------------
def related_artist_pool(sp_user: Spotify, sp_app: Spotify, market: str,
                        seed_artists=10, related_per_seed=8, top_per_related=5) -> List[str]:
    pool: List[str] = []
    id_ok = lambda s: isinstance(s, str) and len(s) == 22

    tops = (safe_call(sp_user.current_user_top_artists, limit=seed_artists, time_range="short_term").get("items", []) +
            safe_call(sp_user.current_user_top_artists, limit=seed_artists, time_range="medium_term").get("items", []))
    seen_rel: Set[str] = set()

    for a in tops[:seed_artists]:
        aid = a.get("id")
        if not id_ok(aid):
            continue
        try:
            rel = spotipy.Spotify(auth_manager=sp_app.auth_manager).artist_related_artists(aid).get("artists", [])[:related_per_seed]
        except Exception:
            continue
        for r in rel:
            rid = r.get("id")
            if not id_ok(rid) or rid in seen_rel:
                continue
            seen_rel.add(rid)
            try:
                tt = sp_app.artist_top_tracks(rid, country=market).get("tracks", [])[:top_per_related]
                pool.extend([t.get("id") for t in tt if t and t.get("id")])
            except Exception:
                continue
    return uniq(pool)

def genre_search_pool(sp_app: Spotify, market: str, per_query=12) -> List[str]:
    queries = [
        'genre:"bollywood" year:2023-2025',
        'genre:"edm" year:2023-2025',
        'tag:new genre:"edm"',
        'tag:new genre:"bollywood"',
        'genre:"dance pop" year:2022-2025',
    ]
    pool: List[str] = []
    for q in queries:
        try:
            res = sp_app.search(q=q, type="track", limit=per_query, market=market)
            tracks = (res.get("tracks") or {}).get("items", []) or []
            pool.extend([t.get("id") for t in tracks if t and t.get("id")])
            time.sleep(0.08)
        except Exception:
            continue
    return uniq(pool)

# -------------- Audio Feature Filter --------------
def audio_filter(sp_app: Spotify, ids: List[str], tempo_range: Tuple[float, float],
                 energy_range: Tuple[float, float]) -> List[str]:
    if not ids:
        return []
    out: List[str] = []

    def fetch_features_cautious(batch: List[str]):
        for size in (25, 10, 5, 1):
            ok = True
            i = 0
            while i < len(batch):
                sub = batch[i:i+size]
                time.sleep(0.12)
                try:
                    feats = sp_app.audio_features(sub)
                except SpotifyException:
                    ok = False
                    break
                except Exception:
                    ok = False
                    break
                else:
                    for tr_id, f in zip(sub, feats):
                        yield (tr_id, f or {})
                i += size
            if ok:
                return

    try:
        capped = ids[:160]
        for group in chunked(capped, 25):
            for tr_id, f in fetch_features_cautious(list(group)) or []:
                tempo = f.get("tempo")
                energy = f.get("energy")
                if tempo is None or energy is None:
                    continue
                if tempo_range[0] <= tempo <= tempo_range[1] and energy_range[0] <= energy <= energy_range[1]:
                    out.append(tr_id)
    except Exception:
        return ids[:80]

    return uniq(out)

# -------------- Discovery Orchestrator ------------
def build_discovery(sp_user: Spotify, sp_app: Spotify, prof: Dict[str, Any],
                    market: str, avoid_ids: Set[str]) -> List[str]:
    recent_played = set(get_recent_track_ids(sp_user, limit=50))
    saved_lib     = set(get_saved_track_ids(sp_user, max_items=300))
    recent_in_pl  = set(recent_playlist_ids(sp_user, PLAYLIST_ID, hours=96))
    avoid = set(avoid_ids) | recent_played | saved_lib | recent_in_pl

    pool: List[str] = []
    pool.extend(related_artist_pool(sp_user, sp_app, market, seed_artists=10, related_per_seed=8, top_per_related=5))
    pool.extend(genre_search_pool(sp_app, market, per_query=12))
    pool = [x for x in pool if x and x not in avoid]
    pool = audio_filter(sp_app, pool, tempo_range=prof["tempo"], energy_range=prof["energy"])
    return uniq(pool)

# -------------- Run History (≤10% repeats) --------
def load_history() -> Tuple[Dict[str, Any], Set[str]]:
    try:
        with open(HISTORY_PATH, "r") as f:
            data = json.load(f)
    except Exception:
        data = {"entries": []}
    cutoff = (dt.datetime.utcnow() - dt.timedelta(days=HISTORY_DAYS)).isoformat()
    data["entries"] = [e for e in data.get("entries", []) if e.get("ts", "") >= cutoff]
    seen: Set[str] = set()
    for e in data["entries"]:
        for tid in e.get("tracks", []):
            if tid:
                seen.add(tid)
    return data, seen

def save_history(history: Dict[str, Any], track_ids: List[str]) -> None:
    history.setdefault("entries", []).append({"ts": dt.datetime.utcnow().isoformat(), "tracks": track_ids})
    try:
        with open(HISTORY_PATH, "w") as f:
            json.dump(history, f, indent=2)
    except Exception:
        pass

# -------------- Run Report (CSV per run) ----------
def write_run_report(sp_app: Spotify,
                     final_ids: List[str],
                     carry: List[str],
                     familiar_pick: List[str],
                     discovery_ids: List[str],
                     history_ids: Set[str],
                     prev_ids: List[str]) -> None:
    if not WRITE_REPORT:
        return
    try:
        os.makedirs("runs", exist_ok=True)
        ts = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path = f"runs/{ts}.csv"

        source = {}
        for t in carry:           source[t] = "carry"
        for t in familiar_pick:   source.setdefault(t, "familiar")
        for t in discovery_ids:   source.setdefault(t, "discovery")

        meta = fetch_track_meta(sp_app, final_ids)
        prev_set = set(prev_ids)
        with open(path, "w", encoding="utf-8") as f:
            f.write("track_id,name,artists,source,was_in_history,was_in_prev_playlist\n")
            for tid in final_ids:
                m = meta.get(tid, {"name": "", "artists": ""})
                name = (m["name"] or "").replace('"', "")
                arts = (m["artists"] or "").replace('"', "")
                src = source.get(tid, "?")
                f.write(f"{tid},\"{name}\",\"{arts}\",{src},{int(tid in history_ids)},{int(tid in prev_set)}\n")
        print(f"REPORT: wrote {path}")
    except Exception:
        pass

# --------------------- Main -----------------------
def main():
    if not PLAYLIST_ID:
        print("PLAYLIST_ID env var is missing")
        return 1

    sp_user = sp_user_client()  # user-scoped (uses refresh token)
    sp_app  = sp_app_client()   # app-scoped (search, features, related, etc.)

    prof = current_profile()
    n_total = prof["n_tracks"]

    # history-based repeat cap
    history, history_ids = load_history()
    repeat_budget = int(math.floor(n_total * MAX_REPEAT_PCT))

    # carry-over (testing: env can force 0.0)
    carry_n = int(math.floor(n_total * CARRY_OVER_PCT))
    current = read_playlist_track_ids(sp_user, PLAYLIST_ID)
    carry = current[:carry_n] if current else []

    # familiar ratio (profile or env override)
    familiar_ratio = float(FAMILIAR_RATIO_OVR) if FAMILIAR_RATIO_OVR else prof["familiar_ratio"]

    # familiar target
    familiar_target = int(math.floor(n_total * familiar_ratio))

    # familiar from user's top tracks (short + medium)
    top_short = safe_call(sp_user.current_user_top_tracks, limit=50, time_range="short_term").get("items", [])
    top_med   = safe_call(sp_user.current_user_top_tracks, limit=50, time_range="medium_term").get("items", [])
    familiar_ids_src = uniq([t.get("id") for t in (top_short + top_med) if t and t.get("id")])

    # prefer never-before-used first, then allow ≤ repeat_budget from history if needed
    familiar_clean = [t for t in familiar_ids_src if t not in carry and t not in history_ids]
    familiar_pick = familiar_clean[:familiar_target]
    if len(familiar_pick) < familiar_target and repeat_budget > 0:
        fillers = [t for t in familiar_ids_src if t not in carry and t in history_ids]
        need = min(familiar_target - len(familiar_pick), repeat_budget)
        familiar_pick += fillers[:need]
        repeat_budget -= need

    # discovery
    remaining = max(0, n_total - len(carry) - len(familiar_pick))
    avoid = set(carry) | set(familiar_pick) | set(history_ids)
    discovery_pool = build_discovery(sp_user, sp_app, prof, MARKET, avoid_ids=avoid)

    discovery_ids = [d for d in discovery_pool if d not in history_ids][:remaining]
    if len(discovery_ids) < remaining and repeat_budget > 0:
        extras = [d for d in discovery_pool if d in history_ids]
        need = min(remaining - len(discovery_ids), repeat_budget)
        discovery_ids += extras[:need]
        repeat_budget -= need

    # final merge
    final_ids  = uniq(carry + familiar_pick + discovery_ids)[:n_total]

    # --- NOVELTY CHECK vs previous run ---
    prev_ids = current  # playlist at start of run
    if prev_ids:
        overlap = len(set(final_ids) & set(prev_ids))
        overlap_pct = overlap / max(1, len(final_ids))
        new_pct = 1.0 - overlap_pct
        print(f"NOVELTY: {new_pct:.1%} new vs previous run ({len(final_ids)-overlap}/{len(final_ids)} new).")

        if new_pct < NOVELTY_MIN_DIFF:
            shortfall = int(math.ceil((NOVELTY_MIN_DIFF - new_pct) * len(final_ids)))
            print(f"NOVELTY fallback: need ~{shortfall} more new tracks; re-sampling…")
            avoid_more = set(history_ids) | set(prev_ids) | set(final_ids)
            extra_pool = build_discovery(sp_user, sp_app, prof, MARKET, avoid_ids=avoid_more)
            extra = [t for t in extra_pool if t not in avoid_more][:shortfall + 10]
            taken = 0
            for i in range(len(final_ids)-1, -1, -1):
                if taken >= len(extra):
                    break
                if final_ids[i] in prev_ids:
                    final_ids[i] = extra[taken]
                    taken += 1
            final_ids = uniq(final_ids)[:n_total]
            overlap = len(set(final_ids) & set(prev_ids))
            new_pct = 1.0 - overlap / max(1, len(final_ids))
            print(f"NOVELTY after fallback: {new_pct:.1%} new.")

    final_uris = [f"spotify:track:{tid}" for tid in final_ids]

    # write: remove-all then add (fresh "Date added")
    existing_ids = read_playlist_track_ids(sp_user, PLAYLIST_ID)
    if existing_ids:
        existing_uris = [f"spotify:track:{tid}" for tid in existing_ids]
        for chunk in chunked(existing_uris, 100):
            try:
                sp_user.playlist_remove_all_occurrences_of_items(PLAYLIST_ID, chunk)
            except Exception:
                pass
            time.sleep(0.10)

    for chunk in chunked(final_uris, 100):
        sp_user.playlist_add_items(PLAYLIST_ID, chunk)
        time.sleep(0.10)

    # persist run to history
    save_history(history, final_ids)

    # write run report CSV
    write_run_report(sp_app, final_ids, carry, familiar_pick, discovery_ids, history_ids, prev_ids)

    # ---- SAFE PRINT (no complex f-string expressions) ----
    window = {
        "n_tracks": len(final_uris),
        "tempo": list(current_profile()["tempo"]),
        "energy": list(current_profile()["energy"]),
        "familiar_ratio": float(FAMILIAR_RATIO_OVR) if FAMILIAR_RATIO_OVR else current_profile()["familiar_ratio"],
    }
    print("OK:", "wrote", len(final_uris), "tracks to", PLAYLIST_ID, "at", now_ist(), ". Window=", json.dumps(window))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
