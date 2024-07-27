import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id=os.environ.get('client_id')
    client_secret=os.environ.get('client_secret')
    client_credentials = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    spotify = spotipy.Spotify(client_credentials_manager=client_credentials)

    data=spotify.playlist_tracks(playlist_id='https://open.spotify.com/playlist/37i9dQZF1DX18jTM2l2fJY')

    client=boto3.client('s3')
    filename='spotify_raw'+str(datetime.now())+'.json'
    client.put_object(
        Bucket='spotify-data-dw',
        Key='raw_data/going_to_process/'+filename,
        Body=json.dumps(data)
        )