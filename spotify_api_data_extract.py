import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def lambda_handler(event, context):
    logger.info("Spotify API data extraction started")
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    client_credential_manager = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    
    sp = spotipy.Spotify(client_credentials_manager=client_credential_manager)
    playlists = sp.user_playlists('spotify')
    
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
    playlist_uri = playlist_link.split("/")[-1]
    
    top50_tracks_data = sp.playlist_tracks(playlist_id=playlist_uri)
    print(top50_tracks_data)
    
    client = boto3.client('s3')
    s3_file_name = "spotify_top50_raw_data_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket="spotify-etl-pipeline-sun10",
        Key="raw_data/to_process/" + s3_file_name,
        Body=json.dumps(top50_tracks_data))