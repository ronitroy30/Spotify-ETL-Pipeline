import json
import boto3
from datetime import datetime
from io import StringIO 
import pandas as pd

def album(data):
 album_frame=[]
 for alb in data['items']:
  album_id=alb['track']['album']['id']
  album_name=alb['track']['album']['name']
  album_release_date=alb['track']['album']['release_date']
  album_total_tracks=alb['track']['album']['total_tracks']
  album_uri=alb['track']['album']['external_urls']["spotify"]
  album_element={'album_id':album_id,'album_name':album_name,'album_release_date':album_release_date,'album_total_tracks':album_total_tracks,'album_uri':album_uri}
  album_frame.append(album_element) 
 return album_frame

def song(data):
 song_frame=[]
 for son in data['items']:
  song_id=son['track']['id']
  song_name=son['track']['name']
  song_duration_ms=son['track']['duration_ms']
  song_popularity=son['track']['popularity']
  song_url=son['track']['external_urls']["spotify"]
  song_added=son['added_at']
  album_id=son['added_at']
  artist_id=son['track']['artists'][0]['external_urls']["spotify"]
  song_element={'song_id':song_id,'song_name':song_name,'song_duration_ms':song_duration_ms,'song_popularity':song_popularity,'song_url':song_url,'song_added':song_added,'album_id':album_id,'artist_id':artist_id}
  song_frame.append(song_element) 
 return song_frame

def artist(data):
 artist_list=[]    
 for row in data['items']:
   for key,value in row.items():
     if key=='track':
          for artist in value['artists']: 
           artist_element={'artist_id':artist['id'],'artist_name':artist['name'],'external_url':artist['href']}
           artist_list.append(artist_element)
 return artist_list
 
def lambda_handler(event, context):
   s3=boto3.client('s3')
   Bucket='spotify-data-dw'
   Key='raw_data/going_to_process/'
   
   spotify_data=[]
   keys=[]
   for ob in s3.list_objects(Bucket=Bucket,Prefix=Key)['Contents']:
       file_key=ob['Key']
       if file_key.split('.')[-1]=='json':
           response=s3.get_object(Bucket=Bucket,Key=file_key)
           content=response['Body']
           json_object=json.loads(content.read())
           spotify_data.append(json_object)
           keys.append(file_key)
           
   for data in spotify_data:
        album_list=album(data)
        song_list=song(data)
        artist_list=artist(data)   
        
        df_song_list=pd.DataFrame.from_dict(song_list)
        df_album_list=pd.DataFrame.from_dict(album_list)
        df_artist_list=pd.DataFrame.from_dict(artist_list)

        df_album_list['album_release_date']=pd.to_datetime(df_album_list['album_release_date'])
        df_song_list['song_added']=pd.to_datetime(df_song_list['song_added'])

        df_album_list=df_album_list.drop_duplicates(subset='album_id')
        
        song_key='transformed_data/song_data/song_transformed_'+ str(datetime.now()) + '.csv'
        song_buffer=StringIO()
        df_song_list.to_csv(song_buffer,index=False)
        song_content=song_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=song_key,Body=song_content)
        
        album_key='transformed_data/album_data/album_transformed_'+ str(datetime.now()) + '.csv'
        album_buffer=StringIO()
        df_album_list.to_csv(album_buffer,index=False)
        album_content=album_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=album_key,Body=album_content)
        
        artist_key='transformed_data/artist_data/artist_transformed_'+ str(datetime.now()) + '.csv'
        artist_buffer=StringIO()
        df_artist_list.to_csv(artist_buffer,index=False)
        artist_content=artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=artist_key,Body=artist_content)
        
        s3_resource=boto3.resource('s3')
        for key in keys:
           copy_source={
            'Bucket':Bucket,
            'Key':key
           }
           s3_resource.meta.client.copy(copy_source,Bucket,'raw_data/processed_data/'+key.split('/')[-1])
           s3_resource.Object(Bucket,key).delete()