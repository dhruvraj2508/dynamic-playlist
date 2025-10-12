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

# ---------- Helpers ----------
ID_RE = re.compile(r"^[0-9A-Za-z]{22}$")

def now_ist():
    return dt.datetime.now(IST)

def current_profile():
    """Time-of-day windows with tempo/energy ranges + familiar target."""
    h = now_ist().hour
    if 10 <= h < 13:     # 10–13 High-energy (work vibe)
        return {"n_tracks": 50, "tempo": (105, 130), "energy": (0.65, 0.85), "familiar_ratio": 0.70}
    elif 13 <= h < 16:   # 13–16 Mellow
        return {"n_tracks": 50, "tempo": (70, 95),   "energy": (0.30, 0.50), "familiar_ratio": 0.70}
    elif 16 <= h < 20:   # 16–20 Focused high-energy
        return {"n_tracks": 50, "tempo": (105, 132), "energy": (0.60, 0.80), "familiar_ratio": 0.70}
    else:                # Off-hours mellow
        return {"n_tracks": 40, "tempo": (70, 95),   "energy": (0.30, 0.50), "familiar_ratio": 0.70}

class RefreshAuth(SpotifyOAuth):
    def __init__(self):
        super().__init__(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            redirect_uri="https://example.com/callback",  # not used again; refresh flow only
            scope="user-top-read playlist-read-private playlist-modify-private playlist-modify-public",
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
    """Filter tracks by audio features."""
    if not ids:
        return []
    out = []
    for batch in chunked(ids, 100):
        feats = safe_call(sp.audio_features, batch)
        for tr_id, f in zip(batch, feats):
            if not f:
                continue
            tempo = f.get("tempo")
            energy = f.get("energy")
            if tempo is None or energy is None:
                continue
            if tempo_range[0] <= tempo <= tempo_range[1] and energy_range[0] <= energy <= energy_range[1]:
                out.append(tr_id)
    return out

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

def genre_search_pool(sp: Spotify, market: str, per_query=25) -> List[str]:
    """
    Use Search API to gather new tracks by keywords (EDM/pop/indie/hip-hop + Indian cues).
    We avoid 'genre:' operator issues by using free-text queries that work broadly.
    """
    queries = [
        # high-energy & Indian cues included
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
        except Exception:
            continue
    return uniq(pool)

def build_discovery(sp: Spotify, prof, market: str, avoid_ids: set) -> List[str]:
    """
    Build discovery without using /recommendations:
      - top artists' top tracks
      - keyword search pools
      - filter by audio features
    """
    pool = []
    pool.extend(top_artist_tracks_pool(sp, market, max_artists=12))
    pool.extend(genre_search_pool(sp, market, per_query=30))

    # Remove carried/familiar tracks
    pool = [x for x in pool if x and x not in avoid_ids]

    # Filter by tempo/energy to match the current window
    pool = audio_filter(sp, pool, tempo_range=prof["tempo"], energy_range=prof["energy"])

    return uniq(pool)

# ---------- Main ----------
def main():
    sp = sp_client()
    prof = current_profile()
    n_total = prof["n_tracks"]

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

    # Replace items in the SAME playlist
    safe_call(sp.playlist_replace_items, PLAYLIST_ID, final_uris)

    print(f"OK: wrote {len(final_uris)} tracks to {PLAYLIST_ID} at {now_ist()}. Window={prof}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
