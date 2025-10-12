# refresh.py — minimal & robust: seeds = current playlist tracks (fallback to safe genres)
# deps: spotipy==2.23.0, pytz  (already in your workflow)

import os, re, datetime as dt
from pytz import timezone
import spotipy
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth

# -------- Settings from GitHub Secrets --------
IST = timezone(os.getenv("TIMEZONE","Asia/Kolkata"))
MARKET = os.getenv("COUNTRY_MARKET","IN")
PLAYLIST_ID = os.environ["PLAYLIST_ID"]
CLIENT_ID = os.environ["SPOTIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SPOTIFY_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["SPOTIFY_REFRESH_TOKEN"]

# -------- Helpers --------
ID_RE = re.compile(r"^[0-9A-Za-z]{22}$")

def now_ist():
    return dt.datetime.now(IST)

def current_profile():
    h = now_ist().hour
    if 10 <= h < 13:     # 10–13 High-energy
        return {"n_tracks":50,"tempo":(105,130),"energy":(0.65,0.85),"familiar_ratio":0.70}
    elif 13 <= h < 16:   # 13–16 Mellow
        return {"n_tracks":50,"tempo":(70,95),  "energy":(0.30,0.50),"familiar_ratio":0.70}
    elif 16 <= h < 20:   # 16–20 Focused high-energy
        return {"n_tracks":50,"tempo":(105,132),"energy":(0.60,0.80),"familiar_ratio":0.70}
    else:                # Off-hours mellow
        return {"n_tracks":40,"tempo":(70,95),  "energy":(0.30,0.50),"familiar_ratio":0.70}

class RefreshAuth(SpotifyOAuth):
    def __init__(self):
        super().__init__(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            redirect_uri="https://example.com/callback",
            scope="user-top-read playlist-read-private playlist-modify-private playlist-modify-public",
            cache_path=None,
            open_browser=False,
        )
    def token(self):
        return self.refresh_access_token(REFRESH_TOKEN)["access_token"]

def sp_client() -> Spotify:
    return Spotify(auth=RefreshAuth().token())

def extract_id(val: str):
    if not val or not isinstance(val, str): return None
    if ID_RE.match(val): return val
    if val.startswith("spotify:"):
        parts = val.split(":")
        if len(parts)>=3 and ID_RE.match(parts[-1]): return parts[-1]
    if "open.spotify.com" in val:
        last = val.strip().split("/")[-1].split("?")[0]
        if ID_RE.match(last): return last
    return None

def uniq(seq):
    seen=set(); out=[]
    for x in seq:
        if x and x not in seen:
            seen.add(x); out.append(x)
    return out

def read_playlist_track_ids(sp: Spotify, playlist_id: str):
    ids = []
    results = sp.playlist_items(playlist_id, additional_types=("track",))
    while True:
        for it in results.get("items", []):
            tr = it.get("track")
            tid = tr and tr.get("id")
            if tid: ids.append(tid)
        if results.get("next"): results = sp.next(results)
        else: break
    return ids

# -------- Core logic --------
def recommendations(sp: Spotify, prof, seed_ids, limit):
    if limit <= 0:
        return []

    # Clean to proper 22-char IDs; take up to 2 track seeds
    seed_ids = [extract_id(x) for x in seed_ids]
    seed_ids = [x for x in seed_ids if x][:2]

    params = {
        "limit": min(100, max(1, limit)),
        "market": MARKET,
        "min_tempo":  prof["tempo"][0],
        "max_tempo":  prof["tempo"][1],
        "min_energy": prof["energy"][0],
        "max_energy": prof["energy"][1],
        "target_tempo": round(sum(prof["tempo"])/2, 1),
        "target_energy": round(sum(prof["energy"])/2, 2),
        "min_popularity": 20,
    }

    # Always provide at least one seed as a LIST
    if seed_ids:
        params["seed_tracks"] = seed_ids
    else:
        # Safe, widely-available genres as LIST (no "bollywood" to avoid 404s)
        params["seed_genres"] = ["pop", "dance", "edm", "indie", "hip-hop"]

    rec = sp.recommendations(**params)
    return [t["id"] for t in rec.get("tracks", []) if t and t.get("id")]

def main():
    sp = sp_client()
    prof = current_profile()
    n_total = prof["n_tracks"]

    # 20% carry-over
    current = read_playlist_track_ids(sp, PLAYLIST_ID)
    carry_n = int(n_total * 0.20)
    carry = current[:carry_n] if current else []

    # Familiar 70% from user's top tracks
    top_short = sp.current_user_top_tracks(limit=50, time_range="short_term").get("items",[])
    top_med   = sp.current_user_top_tracks(limit=50, time_range="medium_term").get("items",[])
    familiar_ids = uniq([t.get("id") for t in (top_short+top_med) if t and t.get("id")])
    familiar_target = int(n_total * prof["familiar_ratio"])
    familiar_pick = [t for t in familiar_ids if t not in carry][:familiar_target]

    # Discovery with seeds from current playlist (most robust)
    remaining = max(0, n_total - len(carry) - len(familiar_pick))
    seed_from_playlist = current[:2]  # use current tracks as seeds
    discovery_ids = [d for d in recommendations(sp, prof, seed_from_playlist, remaining)
                     if d not in carry and d not in familiar_pick][:remaining]

    final_ids = uniq(carry + familiar_pick + discovery_ids)[:n_total]
    final_uris = [f"spotify:track:{tid}" for tid in final_ids]  # write URIs

    sp.playlist_replace_items(PLAYLIST_ID, final_uris)
    print(f"OK: wrote {len(final_uris)} tracks to {PLAYLIST_ID} at {now_ist()}. Window={prof}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
