# Import the lambda handler we want to test
from spotify_api_data_extract import lambda_handler

def test_lambda_handler(mocker):
    """
    Tests that lambda_handler:
    - Uses Spotify client
    - Uploads data to S3
    WITHOUT calling real AWS or Spotify
    """

    # Mock environment variables
    mocker.patch.dict("os.environ", {
        "client_id": "fake_client_id",
        "client_secret": "fake_client_secret"
    })

    # Create a fake Spotify client
    fake_spotify_client = mocker.Mock()

    # Mock playlist_tracks() response
    fake_spotify_client.playlist_tracks.return_value = {"items": []}

    # Replace spotipy.Spotify with fake client
    mocker.patch(
        "api_extract.spotipy.Spotify",
        return_value=fake_spotify_client
    )

    # Mock S3 upload function
    mock_upload = mocker.patch("api_extract.upload_to_s3")

    # Call lambda handler
    lambda_handler({}, {})

    # Assert Spotify API was called
    fake_spotify_client.playlist_tracks.assert_called_once()

    # Assert S3 upload happened
    mock_upload.assert_called_once()

