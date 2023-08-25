api_key='AIzaSyDqSdf8JteKpRqg-iXpEkMn88jffLQc-Zs'
ch_id='UCXzULCWuvbnjm7Q0F6RBKsw'
import googleapiclient.discovery
from pprint import pprint
import pandas as pd
youtube = googleapiclient.discovery.build("youtube", 'v3', developerKey=api_key)
def channel_details(ch_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=ch_id
    )
    response = request.execute()
    dic =dict(playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
              title=response['items'][0]['snippet']['title'],
              description=response['items'][0]['snippet']['description'],
              published_at=response['items'][0]['snippet']['publishedAt'],
              subscribercount=response['items'][0]['statistics']['subscriberCount'],
              videocount=response['items'][0]['statistics']['videoCount'],
              viewcount=response['items'][0]['statistics']['viewCount'])
    return dic            
def playlist_details(ch_ids):
    a=[]
    token=None
    while True:
        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=ch_ids,
                maxResults=25,
                pageToken=token
            )
        response = request.execute()    
        for i in range(len(response['items'])):
            playlist=dict(playlist_id=response['items'][i]['id'],
                        decscription=response['items'][i]['snippet'][ 'localized']['description'],
                        title=response['items'][i]['snippet'][ 'localized']['title'],
                        published_at=response['items'][i]['snippet']['publishedAt'],
                        no_of_videos=response['items'][i]['contentDetails']['itemCount'])
            a.append(playlist)
        token=response.get('nextPageToken')
        if response.get('nextPageToken') is None:
            return(a)
 
def video_id(playlist_main_id):
        b=[]
        token=None
        while True:
                request = youtube.playlistItems().list(
                        part="contentDetails",
                        playlistId = playlist_main_id,
                        maxResults = 50,
                        pageToken = token)
                response = request.execute()
                for i in range (len(response['items'])):
                                videos=(response['items'][i]['contentDetails']['videoId'])
                                b.append(videos)
                token=response.get('nextPageToken')
                if response.get('nextPageToken') is None:
                        return(b)
def video_details(video_ids):
    v=[]
    for i in range(0,len(video_ids),50): 
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(video_ids[i:i+50]))
        response = request.execute()
        #pprint(response)
        for i in range(len(response['items'])):
            video_stats=dict(caption=response['items'][i]['contentDetails']['caption'],
                                definition=response['items'][i]['contentDetails'][ 'definition'],
                                dimension=response['items'][i]['contentDetails'][ 'dimension'],
                                duration=response['items'][i]['contentDetails']['duration'],
                                projection=response['items'][i]['contentDetails']['projection'],
                                categoryId=response['items'][i]['snippet']['categoryId'],
                                video_id=response['items'][i]['id'],
                                videoTitle=response['items'][i]['snippet']["title"],
                                publishedat=response['items'][i]['snippet']["publishedAt"],
                                #defaultAudioLanguage=response['items'][i]['snippet']["defaultAudioLanguage"],
                                likecount=response['items'][i]['statistics']['likeCount'],
                                viewcount=response['items'][i]['statistics']['viewCount'],
                                favorite_count=response['items'][i]['statistics']['favoriteCount'],
                                comment_count=response['items'][i]['statistics']['commentCount'])
            v.append(video_stats)
    return v
def comment_details(video):
    c=[]
    for i in video: 
       try: 
            request = youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=i,
                        maxResults =100
                    )
            response = request.execute()
            for i in (response['items']):
                name = dict( name = i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            video_id=i['snippet']['topLevelComment']['snippet']['videoId'],
                            comment=i['snippet']['topLevelComment'][ "snippet"]['textOriginal'],
                            publishedAt=i['snippet']['topLevelComment']['snippet'][ "publishedAt"],
                            likecount=i['snippet']['topLevelComment']['snippet']['likeCount'])
                c.append(name)
       except:
            pass    
    return c
def main(ch_id):
    c = channel_details(ch_id)
    p = playlist_details(ch_id)
    vi = video_id(playlist_main) 
    v = video_details(vi)
    cm = comment_details(vi)

    data={'channel details':c,
          'playlist details':p,
          'video_id':vi,
          'videodetails':v,
          'comment details':cm}
    return data
channel_details(ch_id)
channel=channel_details(ch_id)
playlist_main=channel['playlist_id']
playlist_details(ch_id)
video_ids=video_id(playlist_main)
video_details(video_ids)
comment=comment_details(video_ids)
m=main(ch_id)
print(m)

m['videodetails']
import pymongo

myclient = pymongo.MongoClient('mongodb+srv://yogavignesh:190asv@cluster0.ge0ozs7.mongodb.net/?retryWrites=true&w=majority')
mydb = myclient["youtube_API"]

mycol = mydb["channel_information"]
mycol.insert_one(m)
# import mysql.connector

# mydb = mysql.connector.connect(
#   host="localhost",
#   user="yourusername",
#   password="yourpassword",
#   database="mydatabase"
# )

# mycursor = mydb.cursor()

# mycursor.execute("CREATE TABLE customers (name VARCHAR(255), address VARCHAR(255))")

                         

                

              
            
        