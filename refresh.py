# refresh.py — robust, no /v1/recommendations calls
# deps: spotipy==2.23.0, pytz  (pin in your workflow)
import os, re, time, math, datetime as dt
from typing import List, Iterable
from pytz import timezone
import spotipy
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException


# ---------- Config from GitHub Secrets ----------
IST = timezone(os.getenv("TIMEZONE", "Asia/Kolkata"))
MARKET = os.getenv("COUNTRY_MARKET", "IN")
PLAYLIST_ID = os.environ["PLAYLIST_ID"]
CLIENT_ID = os.environ["SPOTIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SPOTIFY_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["SPOTIFY_REFRESH_TOKEN"]
HISTORY_PATH = "playlist_history.json"
HISTORY_DAYS = 14  # look-back window


# ---------- Helpers ----------
ID_RE = re.compile(r"^[0-9A-Za-z]{22}$")

def get_recent_track_ids(sp: Spotify, limit=50):
    try:
        res = sp.current_user_recently_played(limit=limit)
        items = res.get("items", [])
        return [it["track"]["id"] for it in items if it.get("track") and it["track"].get("id")]
    except Exception:
        return []

def recent_playlist_ids(sp: Spotify, playlist_id: str, hours=72):
    """Tracks added to THIS playlist recently (avoid reusing them)."""
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours)
    ids = []
    results = sp.playlist_items(playlist_id, additional_types=("track",))
    while True:
        for it in results.get("items", []):
            added_at = it.get("added_at")
            if not added_at:
                continue
            try:
                when = dt.datetime.fromisoformat(added_at.replace("Z", "+00:00"))
            except ValueError:
                continue
            if when >= cutoff:
                tr = it.get("track")
                tid = tr and tr.get("id")
                if tid:
                    ids.append(tid)
        if results.get("next"):
            results = sp.next(results)
        else:
            break
    return ids

import json

def load_history():
    try:
        with open(HISTORY_PATH, "r") as f:
            data = json.load(f)
    except Exception:
        data = {"entries": []}
    # keep only last N days
    cutoff = (dt.datetime.utcnow() - dt.timedelta(days=HISTORY_DAYS)).isoformat()
    data["entries"] = [e for e in data["entries"] if e.get("ts", "") >= cutoff]
    seen = set()
    for e in data["entries"]:
        for tid in e.get("tracks", []):
            seen.add(tid)
    return data, seen

def save_history(history, track_ids):
    history["entries"].append({"ts": dt.datetime.utcnow().isoformat(), "tracks": track_ids})
    try:
        with open(HISTORY_PATH, "w") as f:
            json.dump(history, f, indent=2)
    except Exception:
        pass



def get_saved_track_ids(sp: Spotify, max_items=200):
    got, ids = 0, []
    try:
        while got < max_items:
            page = sp.current_user_saved_tracks(limit=50, offset=got)
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

def related_artist_pool(sp: Spotify, market: str, seed_artists=8, related_per_seed=6, top_per_related=5):
    pool = []
    id_ok = lambda s: isinstance(s, str) and len(s) == 22
    tops = (sp.current_user_top_artists(limit=seed_artists, time_range="short_term").get("items", [])
            + sp.current_user_top_artists(limit=seed_artists, time_range="medium_term").get("items", []))
    seen = set()
    for a in tops[:seed_artists]:
        aid = a.get("id")
        if not id_ok(aid):
            continue
        try:
            rel = sp.artist_related_artists(aid).get("artists", [])[:related_per_seed]
        except Exception:
            continue
        for r in rel:
            rid = r.get("id")
            if not id_ok(rid) or rid in seen:
                continue
            seen.add(rid)
            try:
                tt = sp.artist_top_tracks(rid, country=market).get("tracks", [])[:top_per_related]
                pool.extend([t.get("id") for t in tt if t and t.get("id")])
            except Exception:
                continue
    return uniq(pool)



def now_ist():
    return dt.datetime.now(IST)

def current_profile():
    """Time-of-day windows with tempo/energy ranges + familiar target."""
    h = now_ist().hour
    if 10 <= h < 13:     # 10–13 High-energy (work vibe)
        return {"n_tracks": 50, "tempo": (105, 130), "energy": (0.65, 0.85), "familiar_ratio": 0.60}
    elif 13 <= h < 16:   # 13–16 Mellow
        return {"n_tracks": 50, "tempo": (70, 95),   "energy": (0.30, 0.50), "familiar_ratio": 0.60}
    elif 16 <= h < 20:   # 16–20 Focused high-energy
        return {"n_tracks": 50, "tempo": (105, 132), "energy": (0.60, 0.80), "familiar_ratio": 0.60}
    else:                # Off-hours mellow
        return {"n_tracks": 40, "tempo": (70, 95),   "energy": (0.30, 0.50), "familiar_ratio": 0.60}

class RefreshAuth(SpotifyOAuth):
    def __init__(self):
        super().__init__(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            redirect_uri="https://example.com/callback",
            scope=(
                "user-top-read "
                "playlist-read-private playlist-modify-private playlist-modify-public "
                "user-read-recently-played user-library-read"   # ← add these
            ),
            cache_path=None,
            open_browser=False,
        )

    def token(self):
        return self.refresh_access_token(REFRESH_TOKEN)["access_token"]

def sp_client() -> Spotify:
    return Spotify(auth=RefreshAuth().token())

def extract_id(val: str):
    """Normalize ID/URI/URL → bare 22-char track ID; else None."""
    if not val or not isinstance(val, str):
        return None
    if ID_RE.match(val):
        return val
    if val.startswith("spotify:"):
        parts = val.split(":")
        cand = parts[-1] if len(parts) >= 3 else ""
        return cand if ID_RE.match(cand) else None
    if "open.spotify.com" in val:
        last = val.strip().split("/")[-1].split("?")[0]
        return last if ID_RE.match(last) else None
    return None

def uniq(seq: Iterable[str]) -> List[str]:
    seen = set(); out = []
    for x in seq:
        if x and x not in seen:
            seen.add(x); out.append(x)
    return out

# ---------- Spotify fetch utilities ----------
def read_playlist_track_ids(sp: Spotify, playlist_id: str) -> List[str]:
    ids = []
    results = sp.playlist_items(playlist_id, additional_types=("track",))
    while True:
        for it in results.get("items", []):
            tr = it.get("track")
            tid = tr and tr.get("id")
            if tid:
                ids.append(tid)
        if results.get("next"):
            results = sp.next(results)
        else:
            break
    return ids

def chunked(iterable: List[str], size=50):
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]

def safe_call(fn, *args, **kwargs):
    """Simple retry wrapper for occasional transient API hiccups."""
    tries = 3
    delay = 0.8
    for attempt in range(tries):
        try:
            return fn(*args, **kwargs)
        except SpotifyException as e:
            if attempt == tries - 1:
                raise
            time.sleep(delay)
            delay *= 1.6

# ---------- Discovery building without Recommendations ----------
def audio_filter(sp: Spotify, ids: List[str], tempo_range, energy_range) -> List[str]:
    """Filter tracks by audio features with cautious batching; on repeated 403s, fall back silently."""
    if not ids:
        return []
    out: List[str] = []

    def fetch_features_safely(batch: List[str]) -> List[tuple]:
        # progressively smaller chunks; never log, just downshift/skip
        chunk_sizes = [25, 10, 5, 1]
        for size in chunk_sizes:
            ok = True
            for i in range(0, len(batch), size):
                sub = batch[i:i+size]
                time.sleep(0.12)
                try:
                    feats = sp.audio_features(sub)
                except SpotifyException as e:
                    # 403/429: try smaller chunk
                    ok = False
                    break
                except Exception:
                    ok = False
                    break
                else:
                    for tr_id, f in zip(sub, feats):
                        yield (tr_id, f)
            if ok:
                return
        return

    try:
        capped = ids[:160]  # keep API load modest
        for i in range(0, len(capped), 25):
            batch = capped[i:i+25]
            for tr_id, f in fetch_features_safely(batch) or []:
                if not f:
                    continue
                tempo = f.get("tempo"); energy = f.get("energy")
                if tempo is None or energy is None:
                    continue
                if tempo_range[0] <= tempo <= tempo_range[1] and energy_range[0] <= energy <= energy_range[1]:
                    out.append(tr_id)
    except Exception:
        # total fallback: if audio features keep failing, just return a trimmed list unfiltered
        return ids[:80]

    # dedupe preserve order
    seen=set(); keep=[]
    for t in out:
        if t not in seen:
            seen.add(t); keep.append(t)
    return keep



def top_artist_tracks_pool(sp: Spotify, market: str, max_artists=10) -> List[str]:
    """Collect candidates from user's top artists' top tracks."""
    pool = []
    arts = safe_call(sp.current_user_top_artists, limit=max_artists, time_range="short_term").get("items", [])
    for a in arts:
        aid = a.get("id")
        if not aid:
            continue
        try:
            tt = safe_call(sp.artist_top_tracks, aid, country=market).get("tracks", [])
            pool.extend([t.get("id") for t in tt if t and t.get("id")])
        except Exception:
            continue
    return uniq(pool)

def genre_search_pool(sp: Spotify, market: str, per_query=12) -> List[str]:
    queries = [
        "edm 2023..2025", "dance 2023..2025", "club 2023..2025",
        "pop 2023..2025", "indie 2022..2025", "hip hop 2022..2025",
        "hindi pop 2022..2025", "punjabi 2022..2025", "desi pop 2022..2025",
    ]
    pool = []
    for q in queries:
        try:
            res = safe_call(sp.search, q=q, type="track", limit=per_query, market=market)
            items = (res.get("tracks") or {}).get("items", [])
            pool.extend([t.get("id") for t in items if t and t.get("id")])
            time.sleep(0.15)  # tiny pause to be polite
        except Exception:
            continue
    return uniq(pool)


def build_discovery(sp: Spotify, prof, market: str, avoid_ids: set) -> List[str]:
    # Heard before (avoid): carry/familiar + recently played + saved + recently added to THIS playlist
    recent_played = set(get_recent_track_ids(sp, limit=50))
    saved_lib     = set(get_saved_track_ids(sp, max_items=300))
    recent_in_pl  = set(recent_playlist_ids(sp, PLAYLIST_ID, hours=96))  # avoid last 4 days

    avoid = set(avoid_ids) | recent_played | saved_lib | recent_in_pl

    # Candidate pools:
    pool = []
    pool.extend(related_artist_pool(sp, market, seed_artists=10, related_per_seed=8, top_per_related=5))
    pool.extend(genre_search_pool(sp, market, per_query=12))  # small pulls → lower API pressure

    # Remove already-heard / already-used
    pool = [x for x in pool if x and x not in avoid]

    # Filter by audio features (with cautious batching)
    pool = audio_filter(sp, pool, tempo_range=prof["tempo"], energy_range=prof["energy"])
    return uniq(pool)



# ---------- Main ----------
def main():
    sp = sp_client()
    prof = current_profile()
    n_total = prof["n_tracks"]
    history, history_ids = load_history()
    repeat_budget = int(math.floor(n_total * 0.10))  # ≤10% repeats allowed


    # 20% carry-over from current playlist
    current = read_playlist_track_ids(sp, PLAYLIST_ID)
    carry_n = int(math.floor(n_total * 0.20))
    carry = current[:carry_n] if current else []

    # Familiar 70% from user's Top Tracks (short + medium term)
    top_short = safe_call(sp.current_user_top_tracks, limit=50, time_range="short_term").get("items", [])
    top_med   = safe_call(sp.current_user_top_tracks, limit=50, time_range="medium_term").get("items", [])
    familiar_ids = uniq([t.get("id") for t in (top_short + top_med) if t and t.get("id")])
    familiar_target = int(math.floor(n_total * prof["familiar_ratio"]))
    familiar_pick = [t for t in familiar_ids if t not in carry][:familiar_target]

    # Discovery = from top artists' tracks + keyword search, then audio-filter
    remaining = max(0, n_total - len(carry) - len(familiar_pick))
    avoid = set(carry) | set(familiar_pick)
    discovery_pool = build_discovery(sp, prof, MARKET, avoid_ids=avoid)
    discovery_ids = [d for d in discovery_pool if d not in avoid][:remaining]

        # Final merge
    final_ids = uniq(carry + familiar_pick + discovery_ids)[:n_total]
    final_uris = [f"spotify:track:{tid}" for tid in final_ids]  # write URIs

    # --- FORCE FRESH "DATE ADDED": remove then add in chunks ---
    existing_ids = read_playlist_track_ids(sp, PLAYLIST_ID)
    if existing_ids:
        existing_uris = [f"spotify:track:{tid}" for tid in existing_ids]
        for chunk in chunked(existing_uris, 100):
            try:
                sp.playlist_remove_all_occurrences_of_items(PLAYLIST_ID, chunk)
            except Exception:
                pass  # continue even if a chunk fails

    # add the new sequence
    for chunk in chunked(final_uris, 100):
        sp.playlist_add_items(PLAYLIST_ID, chunk)

    print(f"OK: wrote {len(final_uris)} tracks to {PLAYLIST_ID} at {now_ist()}. Window={{'n_tracks': n_total, 'tempo': prof['tempo'], 'energy': prof['energy'], 'familiar_ratio': prof['familiar_ratio']}}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
