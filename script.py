import requests
import pandas as pd
import time

# Function to get Spotify access token
def get_spotify_token(client_id, client_secret):
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_response = requests.post(auth_url, {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    auth_data = auth_response.json()
    return auth_data.get('access_token')

# Function to search for a track and get its ID
def search_track(track_name, artist_name, token):
    query = f"{track_name} artist:{artist_name}"
    url = f"https://api.spotify.com/v1/search?q={query}&type=track&limit=1"
    response = requests.get(url, headers={'Authorization': f'Bearer {token}'})
    json_data = response.json()
    try:
        first_result = json_data['tracks']['items'][0]
        return first_result['id']
    except (KeyError, IndexError):
        return None

# Function to get track details (image URL)
def get_track_details(track_id, token):
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    response = requests.get(url, headers={'Authorization': f'Bearer {token}'})
    json_data = response.json()
    try:
        return json_data['album']['images'][0]['url']
    except (KeyError, IndexError):
        return None

# Your Spotify API Credentials
client_id = 'ae7747ad946748a2968f137ccbe4257f'
client_secret = 'e68a79e6b5a14c52a1b18d83f84b84a5'

# Get Access Token
access_token = get_spotify_token(client_id, client_secret)
if not access_token:
    raise Exception("Failed to get access token. Check your credentials.")

# Read your dataset
file_path = r"C:\Users\91771\OneDrive\Documents\powerbi_project\spotyy\spotify-2023.csv"
df_spotify = pd.read_csv(file_path, encoding='ISO-8859-1')

# Ensure required columns exist
if 'track_name' not in df_spotify.columns or 'artist(s)_name' not in df_spotify.columns:
    raise Exception("Required columns ('track_name', 'artist(s)_name') not found in dataset.")

# Loop through each row to get track details and add to DataFrame
df_spotify['image_url'] = None  # Initialize new column

for i, row in df_spotify.iterrows():
    track_name = row['track_name']
    artist_name = row['artist(s)_name']
    
    print(f"Searching for: {track_name} by {artist_name}")

    track_id = search_track(track_name, artist_name, access_token)
    if track_id:
        print(f"Found track ID: {track_id}")
        image_url = get_track_details(track_id, access_token)
        df_spotify.at[i, 'image_url'] = image_url
    else:
        print(f"Track not found: {track_name} by {artist_name}")

    # Save every 10 rows to avoid data loss
    if i % 10 == 0:
        df_spotify.to_csv("updated_file.csv", index=False)

    time.sleep(1)  # Prevent API rate limits

# Save the final updated DataFrame
df_spotify.to_csv("updated_file.csv", index=False)
print("Updated file saved as 'updated_file.csv'")
