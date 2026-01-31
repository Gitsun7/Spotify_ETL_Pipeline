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


def get_spotify_client():
    """
    Creates and returns a Spotify client using credentials from env variables.
    This function is isolated so it can be mocked in tests.
    """
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    credential_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    )

    return spotipy.Spotify(client_credentials_manager=credential_manager)


def upload_to_s3(data, bucket, key):
    """
    Uploads JSON data to S3.
    Isolated so we can mock boto3 easily.
    """
    s3 = boto3.client("s3")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data)
    )


def lambda_handler(event, context):
    logger.info("Spotify API data extraction started")

    # Get Spotify client
    sp = get_spotify_client()

    # Playlist URI extraction
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
    playlist_uri = playlist_link.split("/")[-1]

    # Fetch playlist tracks
    top50_tracks_data = sp.playlist_tracks(playlist_id=playlist_uri)

    # Generate S3 file name
    s3_file_name = f"spotify_top50_raw_data_{datetime.now()}.json"

    # Upload raw data to S3
    upload_to_s3(
        data=top50_tracks_data,
        bucket="spotify-etl-pipeline-sun10",
        key="raw_data/to_process/" + s3_file_name
    )
