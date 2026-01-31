# Import pytest for testing
import pytest

# Import ONLY the functions we want to test
from spotify_transform_load import get_albums, get_artists, get_songs

@pytest.fixture
def sample_raw_data():
    """
    This fixture returns a fake Spotify API response.
    Fixtures allow reuse across multiple tests.
    """
    return {
        "items": [
            {
                "added_at": "2024-01-01T10:00:00Z",
                "track": {
                    "id": "song_1",
                    "name": "My Song",
                    "duration_ms": 210000,
                    "popularity": 85,
                    "external_urls": {"spotify": "song_url"},
                    "album": {
                        "id": "album_1",
                        "name": "My Album",
                        "release_date": "2023-01-01",
                        "total_tracks": 10,
                        "external_urls": {"spotify": "album_url"},
                        "artists": [
                            {"id": "artist_1"}
                        ]
                    },
                    "artists": [
                        {
                            "id": "artist_1",
                            "name": "My Artist",
                            "href": "artist_url"
                        }
                    ]
                }
            }
        ]
    }

def test_get_albums(sample_raw_data):
    """
    Tests that album data is correctly extracted from raw Spotify data.
    """

    # Call the function under test
    albums = get_albums(sample_raw_data)

    # Verify one album is returned
    assert len(albums) == 1

    # Extract the album dictionary
    album = albums[0]

    # Validate individual fields
    assert album["album_id"] == "album_1"
    assert album["album_name"] == "My Album"
    assert album["album_release_date"] == "2023-01-01"
    assert album["album_total_tracks"] == 10
    assert album["album_url"] == "album_url"

def test_get_artists(sample_raw_data):
    """
    Tests that artist information is extracted correctly.
    """

    artists = get_artists(sample_raw_data)

    assert len(artists) == 1

    artist = artists[0]

    assert artist["artist_id"] == "artist_1"
    assert artist["artist_name"] == "My Artist"
    assert artist["external_url"] == "artist_url"

def test_get_songs(sample_raw_data):
    """
    Tests that song-level fields are extracted correctly.
    """

    songs = get_songs(sample_raw_data)

    assert len(songs) == 1

    song = songs[0]

    assert song["song_id"] == "song_1"
    assert song["song_name"] == "My Song"
    assert song["song_duration"] == 210000
    assert song["popularity"] == 85
    assert song["album_id"] == "album_1"
    assert song["artist_id"] == "artist_1"
