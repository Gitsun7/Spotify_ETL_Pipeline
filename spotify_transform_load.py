import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def get_albums(raw_data):
    album_list = []
    for albums in raw_data['items']:
        album_id = albums['track']['album']['id']
        album_name = albums['track']['album']['name']
        album_release_date = albums['track']['album']['release_date']
        album_total_tracks = albums['track']['album']['total_tracks']
        album_url = albums['track']['album']['external_urls']['spotify']
        
        album_dict = {'album_id':album_id,'album_name':album_name,'album_release_date':album_release_date,'album_total_tracks':album_total_tracks,'album_url':album_url}
        album_list.append(album_dict)
    return album_list

def get_artists(raw_data):
    artist_list = []
    for track in raw_data['items']:
        for key,value in track.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_dict = {'artist_id':artist['id'],'artist_name':artist['name'],'external_url':artist['href']}
                    artist_list.append(artist_dict)
    return artist_list

def get_songs(raw_data):
    songs_list = []
    for track in raw_data['items']:
        song_id = track['track']['id']
        song_name = track['track']['name']
        song_duration = track['track']['duration_ms']
        song_url = track['track']['external_urls']['spotify']
        song_popularity = track['track']['popularity']
        song_added = track['added_at']
        album_id = track['track']['album']['id']
        artist_id = track['track']['album']['artists'][0]['id']
        
        song_dict = {'song_id':song_id,'song_name':song_name,'song_duration':song_duration,'url':song_url,'popularity':song_popularity,'song_added':song_added,'album_id':album_id,'artist_id':artist_id}
        songs_list.append(song_dict)
    return songs_list
    
def lambda_handler(event, context):
    s3 = boto3.client("s3")
    Bucket = "spotify-etl-pipeline-sun10"
    Key = "raw_data/to_process/"
    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket=Bucket,Prefix=Key)['Contents']:
        file_key = file['Key']
        
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket=Bucket,Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            
            
    for raw_data in spotify_data:
        album_list = get_albums(raw_data)
        artist_list = get_artists(raw_data)
        songs_list = get_songs(raw_data)
        
        albums_df = pd.DataFrame.from_dict(album_list)
        artists_df = pd.DataFrame.from_dict(artist_list)
        songs_df = pd.DataFrame.from_dict(songs_list)
        
        albums_df['album_release_date'] = pd.to_datetime(albums_df['album_release_date'])
        songs_df['song_added'] = pd.to_datetime(songs_df['song_added'])
        
        albums_key = "transformed_data/albums_data/albums_transformed_" + str(datetime.now()) + ".csv"
        albums_buffer = StringIO()
        albums_df.to_csv(albums_buffer,index=False)
        albums_content = albums_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=albums_key,Body=albums_content)

        songs_key = "transformed_data/songs_data/songs_transformed_" + str(datetime.now()) + ".csv"
        songs_buffer = StringIO()
        songs_df.to_csv(songs_buffer,index=False)
        songs_content = songs_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=songs_key,Body=songs_content)

        artists_key = "transformed_data/artists_data/artists_transformed_" + str(datetime.now()) + ".csv"
        artists_buffer = StringIO()
        artists_df.to_csv(artists_buffer,index=False)
        artists_content = artists_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=artists_key,Body=artists_content)
        
    s3_resource = boto3.resource("s3")
    for f_key in spotify_keys:
        copy_source = {"Bucket" : Bucket, "Key" : f_key}
        s3_resource.meta.client.copy(copy_source,Bucket,"raw_data/processed/" + f_key.split('/')[-1])
        s3_resource.Object(Bucket,f_key).delete()