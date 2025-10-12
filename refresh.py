# refresh.py
# pip dependencies: spotipy pytz
import os, datetime as dt
from pytz import timezone
import spotipy
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth

import re
ID_RE = re.compile(r"^[0-9A-Za-z]{22}$")

def extract_id(value: str):
    """Return a clean 22-char Spotify ID from an ID, URI, or URL; else None."""
    if not value or not isinstance(value, str):
        return None
    # Bare ID
    if ID_RE.match(value):
        return value
    # URI: spotify:artist:<id> or spotify:track:<id>
    if value.startswith("spotify:"):
        parts = value.split(":")
        if len(parts) >= 3 and ID_RE.match(parts[-1]):
            return parts[-1]
    # URL: https://open.spotify.com/{type}/<id>(?... )
    if "open.spotify.com" in value:
        last = value.strip().split("/")[-1].split("?")[0]
        if ID_RE.match(last):
            return last
    return None

def clean_seed_list(values):
    """Map list of ids/uris/urls → unique valid IDs (max 5)."""
    out, seen = [], set()
    for v in values or []:
        vid = extract_id(v)
        if vid and vid not in seen:
            seen.add(vid)
            out.append(vid)
        if len(out) >= 5:
            break
    return out

IST = timezone(os.getenv("TIMEZONE","Asia/Kolkata"))
MARKET = os.getenv("COUNTRY_MARKET","IN")
PLAYLIST_ID = os.environ["PLAYLIST_ID"]

CLIENT_ID = os.environ["SPOTIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SPOTIFY_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["SPOTIFY_REFRESH_TOKEN"]

def now_ist():
    return dt.datetime.now(IST)

def current_profile():
    h = now_ist().hour
    if 10 <= h < 13:     # 10–13 High-energy
        return {"n_tracks":50,"tempo":(105,130),"energy":(0.65,0.85),"valence":(0.50,0.75),"familiar_ratio":0.70}
    elif 13 <= h < 16:   # 13–16 Mellow
        return {"n_tracks":50,"tempo":(70,95),  "energy":(0.30,0.50),"valence":(0.35,0.60),"familiar_ratio":0.70}
    elif 16 <= h < 20:   # 16–20 Focused high-energy
        return {"n_tracks":50,"tempo":(105,132),"energy":(0.60,0.80),"valence":(0.45,0.70),"familiar_ratio":0.70}
    else:                # Off-hours mellow
        return {"n_tracks":40,"tempo":(70,95),  "energy":(0.30,0.50),"valence":(0.35,0.60),"familiar_ratio":0.70}

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
    token = RefreshAuth().token()
    return Spotify(auth=token)

def read_playlist_track_ids(sp: Spotify, playlist_id: str):
    ids = []
    results = sp.playlist_items(playlist_id, additional_types=("track",))
    while True:
        for it in results.get("items", []):
            tr = it.get("track")
            if tr and tr.get("id"): ids.append(tr["id"])
        if results.get("next"): results = sp.next(results)
        else: break
    return ids

def uniq(seq):
    seen=set(); out=[]
    for x in seq:
        if x and x not in seen: seen.add(x); out.append(x)
    return out

def pick_seeds(sp: Spotify):
    # Pull top artists/tracks (short_term)
    top_art = sp.current_user_top_artists(limit=20, time_range="short_term").get("items", [])
    top_trk = sp.current_user_top_tracks(limit=20, time_range="short_term").get("items", [])

    # Get whatever identifier is available (id or uri)
    raw_artists = [a.get("id") or a.get("uri") for a in top_art]
    raw_tracks  = [t.get("id") or t.get("uri") for t in top_trk]

    # Clean to bare IDs and cap to 3 artists + 2 tracks
    seed_art = clean_seed_list(raw_artists)[:3]
    seed_trk = clean_seed_list(raw_tracks)[:2]
    return seed_art, seed_trk

def recs(sp, prof, seed_art, seed_trk, limit):
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
        # ← Use only genre seeds to avoid any URI/ID parsing issues
        "seed_genres": "pop,edm,bollywood"
    }
    r = sp.recommendations(**params)
    return [t["id"] for t in r.get("tracks", []) if t and t.get("id")]



def main():
    sp = sp_client()
    prof = current_profile()
    n_total = prof["n_tracks"]

    # carry 20%
    current = read_playlist_track_ids(sp, PLAYLIST_ID)
    keep_n = int(n_total * 0.20)
    carry = current[:keep_n] if current else []

    # familiar 70%
    seed_art, seed_trk = pick_seeds(sp)
    top_short = sp.current_user_top_tracks(limit=50, time_range="short_term").get("items",[])
    top_med   = sp.current_user_top_tracks(limit=50, time_range="medium_term").get("items",[])
    familiar_ids = uniq([t["id"] for t in (top_short+top_med) if t and t.get("id")])
    familiar_target = int(n_total * prof["familiar_ratio"])
    familiar_pick = [t for t in familiar_ids if t not in carry][:familiar_target]

    # discovery
    remaining = max(0, n_total - len(carry) - len(familiar_pick))
    discovery_ids = [d for d in recs(sp, prof, seed_art, seed_trk, remaining) if d not in carry and d not in familiar_pick][:remaining]

    final_ids = uniq(carry + familiar_pick + discovery_ids)[:n_total]
    final_uris = [f"spotify:track:{tid}" for tid in final_ids]
    sp.playlist_replace_items(PLAYLIST_ID, final_uris)


    print(f"Refreshed {PLAYLIST_ID} with {len(final_ids)} tracks at {now_ist()} ({prof})")

if __name__ == "__main__":
    main()
