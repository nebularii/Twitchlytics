import requests
import pandas as pd
import time
import re
from datetime import datetime

# --- CONFIG ---
CLIENT_ID = 's7bv5ln624axo0tknuvmjvjfkrmkzy'
CLIENT_SECRET = 'xuy0bfos8qx569rgy709o0boce2tw6'
MAX_STREAMERS = 100
MAX_VIDEOS_PER_STREAMER = 5

# --- AUTH ---
auth_url = 'https://id.twitch.tv/oauth2/token'
auth_params = {
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'grant_type': 'client_credentials'
}
auth_response = requests.post(auth_url, params=auth_params)
access_token = auth_response.json()['access_token']

headers = {
    'Client-ID': CLIENT_ID,
    'Authorization': f'Bearer {access_token}'
}

# --- HELPERS ---
def parse_duration(duration_str):
    match = re.match(r'(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?', duration_str)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 60 + minutes + seconds / 60

def clean_text(text):
    if not isinstance(text, str):
        return ''
    return re.sub(r'[^\w\s.,!?@#&\'"-]', '', text)

def get_follower_count(user_id):
    try:
        url = 'https://api.twitch.tv/helix/users/follows'
        params = {'to_id': user_id}
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 200:
            return resp.json().get('total', 0)
    except:
        pass
    return 0

def get_tag_names(tag_ids):
    tag_names = []
    if not tag_ids:
        return tag_names
    url = 'https://api.twitch.tv/helix/tags/streams'
    for tag_id in tag_ids:
        resp = requests.get(url, headers=headers, params={'tag_id': tag_id})
        if resp.status_code == 200:
            data = resp.json().get('data', [])
            if data:
                tag_names.append(data[0].get('localization_names', {}).get('en', ''))
        time.sleep(0.1)  # to avoid throttling
    return tag_names

def get_game_name(game_id):
    if not game_id:
        return ''
    url = 'https://api.twitch.tv/helix/games'
    params = {'id': game_id}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        data = resp.json().get('data', [])
        return data[0]['name'] if data else ''
    return ''

# --- GET TOP STREAMS ---
streams = []
url = 'https://api.twitch.tv/helix/streams'
params = {'first': 100}
print("📡 Fetching top live streams...")

while len(streams) < MAX_STREAMERS:
    r = requests.get(url, headers=headers, params=params)
    data = r.json()
    streams.extend(data['data'])

    if 'pagination' in data and 'cursor' in data['pagination']:
        params['after'] = data['pagination']['cursor']
    else:
        break
    time.sleep(1)

streams = streams[:MAX_STREAMERS]
today = datetime.today().strftime('%Y-%m-%d')
stream_data = []

# --- LOOP STREAMERS ---
print(f"📊 Pulling metrics for {len(streams)} streamers...")
for s in streams:
    user_id = s['user_id']
    user_name = clean_text(s['user_name'])
    raw_title = s.get('title', '')
    clean_title = clean_text(raw_title)

    # -- TAGS --
    tag_ids = s.get('tag_ids', [])
    tag_names = get_tag_names(tag_ids)

    # -- FOLLOWERS --
    follower_count = get_follower_count(user_id)

    # -- VIDEOS (ARCHIVES) --
    video_url = 'https://api.twitch.tv/helix/videos'
    video_params = {'user_id': user_id, 'type': 'archive', 'first': MAX_VIDEOS_PER_STREAMER}
    video_resp = requests.get(video_url, headers=headers, params=video_params)
    videos = video_resp.json().get('data', [])

    total_views = sum(v['view_count'] for v in videos)
    total_duration = sum(parse_duration(v['duration']) for v in videos)
    avg_viewers = total_views / len(videos) if videos else 0
    avg_duration = total_duration / len(videos) if videos else 0
    vod_count = len(videos)

    # -- Most Streamed Game --
    game_counts = {}
    for v in videos:
        g_id = v.get('game_id')
        if g_id:
            g_name = get_game_name(g_id)
            if g_name:
                game_counts[g_name] = game_counts.get(g_name, 0) + 1
    most_common_game = max(game_counts, key=game_counts.get) if game_counts else ''

    # -- COLLECT --
    stream_data.append({
        'Display Name': user_name,
        'Title': clean_title,
        'Game ID': s.get('game_id'),
        'Game Name': s.get('game_name'),
        'Viewer Count': s.get('viewer_count'),
        'Language': s.get('language'),
        'Tags': ', '.join(tag_names),
        'Follower Count': follower_count,
        'Avg Past Viewers': round(avg_viewers, 2),
        'Avg Past Stream Duration (min)': round(avg_duration, 2),
        'Recent VOD Count': vod_count,
        'Most Streamed Game (Past VODs)': most_common_game,
        'Date Pulled': today
    })

    time.sleep(0.5)

# --- SAVE TO CSV ---
df = pd.DataFrame(stream_data)
df.to_csv(f'top_{MAX_STREAMERS}_streamers_{today}.csv', index=False, encoding='utf-8-sig')
print(f"✅ Saved to top_{MAX_STREAMERS}_streamers_{today}.csv")


