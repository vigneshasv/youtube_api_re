import googleapiclient.discovery
import pandas as pd
import re
import pymongo
import pymysql
from sqlalchemy import create_engine

def channel_details(ch_id ,youtube):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=ch_id
    )
    response = request.execute()
    dic =dict(playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            channel_name=response['items'][0]['snippet']['title'],
            channel_id=response['items'][0]['id'],
            description=response['items'][0]['snippet']['description'],
            published_at=str((response['items'][0]['snippet']['publishedAt']).split('T')[0]),
            subscribercount=int(response['items'][0]['statistics']['subscriberCount']),
            videocount=int(response['items'][0]['statistics']['videoCount']),
            viewcount=int(response['items'][0]['statistics']['viewCount']))
    return dic


def playlist_details(ch_id ,youtube):
    a=[]
    token=None
    while True:
        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=ch_id,
                maxResults=25,
                pageToken=token
            )
        response = request.execute()    
        for i in range(len(response['items'])):
         try:
            playlist=dict(playlist_id=response['items'][i]['id'],
                        channel_name=response['items'][i]['snippet'][ 'channelTitle'],
                        channel_id= response['items'][i]['snippet'][ 'channelId'],
                        decscription=response['items'][i]['snippet'][ 'localized']['description'],
                        title=response['items'][i]['snippet'][ 'localized']['title'],
                        published_at=(response['items'][i]['snippet']['publishedAt']),
                        no_of_videos=int(response['items'][i]['contentDetails']['itemCount']))
            a.append(playlist)
         except:
             pass   
        token=response.get('nextPageToken')
        if response.get('nextPageToken') is None:
            return(a)
        
        
def video_id(playlist_main_id, youtube):
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
                    
                    
                    
def convert_sec(duration):
    regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match = re.match(regex, duration)
    
    if not match:
        return 0
    
    hours, minutes, seconds = match.groups()
    hours = int(hours[:-1]) if hours else 0
    minutes = int(minutes[:-1]) if minutes else 0
    seconds = int(seconds[:-1]) if seconds else 0
    
    total_seconds = hours * 3600 + minutes * 60 + seconds
    
    return total_seconds

def video_details(video_ids, youtube):
    v=[]
    for i in range(0,len(video_ids),50): 
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(video_ids[i:i+50]))
        response = request.execute()
        #pprint(response)
        for i in range(len(response['items'])):
         try:
            video_stats=dict(caption=response['items'][i]['contentDetails']['caption'],
                                definition=response['items'][i]['contentDetails'][ 'definition'],
                                dimension=response['items'][i]['contentDetails'][ 'dimension'],
                                duration=convert_sec(response['items'][i]['contentDetails']['duration']),
                                projection=response['items'][i]['contentDetails']['projection'],
                                categoryId=response['items'][i]['snippet']['categoryId'],
                                channel_id=response['items'][i]['snippet']['channelId'],
                                video_id=response['items'][i]['id'],
                                videoTitle=response['items'][i]['snippet']["title"],
                                publishedat=(response['items'][i]['snippet']["publishedAt"]),
                                #defaultAudioLanguage=response['items'][i]['snippet']["defaultAudioLanguage"],
                                likecount=int(response['items'][i]['statistics']['likeCount']),
                                viewcount=int(response['items'][i]['statistics']['viewCount']),
                                favorite_count=int(response['items'][i]['statistics']['favoriteCount']),
                                comment_count=int(response['items'][i]['statistics']['commentCount']))
            v.append(video_stats)
         except:
             pass
    return v


def comment_details(video, youtube):
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
                            publishedAt=(i['snippet']['topLevelComment']['snippet'][ "publishedAt"]),
                            likecount=int(i['snippet']['topLevelComment']['snippet']['likeCount']))
                c.append(name)
        except:
            pass    
    return c


def main(ch_id, youtube):
    c = channel_details(ch_id, youtube)
    playlist_main=c['playlist_id']
    p = playlist_details(ch_id, youtube)
    vi = video_id(playlist_main, youtube) 
    v = video_details(vi, youtube)
    cm = comment_details(vi, youtube)

    data={'channel details':c,
        'playlist details':p,
        'video_id':vi,
        'videodetails':v,
        'comment details':cm}
    return data

def get_data(ch_id , api_key):
    youtube = googleapiclient.discovery.build("youtube", 'v3', developerKey=api_key)
    
    m=main(ch_id, youtube)

    myclient = pymongo.MongoClient('mongodb+srv://yogavignesh:190asv@cluster0.ge0ozs7.mongodb.net/')
    mydb = myclient["youtube_API2"]

    mycol = mydb["channel_information"]
    mycol.insert_one(m)

    # Retrieve data from MongoDB collection
    information = mycol.find()

    # Process and use the data as needed
    for i in information:
        # Assuming document is a dictionary
        field1_value = i.get('channel details')
        field2_value = i.get('playlist details')
        field3_value = i.get('videodetails')
        field4_value = i.get('comment details')
    
        global mcd, mpd, mvd, mcod
        
        mcd=pd.DataFrame.from_dict(field1_value,orient='index').T
        mpd=pd.DataFrame(field2_value)
        mvd=pd.DataFrame(field3_value)
        mcod=pd.DataFrame(field4_value)
        
        
def migrate_data():
    #insert retervie data from pymongo,insert into mysql
    mydb = pymysql.connect(
    host = 'localhost',
    user = 'root',
    password = 'yoga1234$')
    global cursor
    cursor = mydb.cursor()
    cursor.execute("CREATE DATABASE if not exists youtubeapi")
    cursor.execute('USE youtubeapi')
    # import the necessary packages
    
    # Create the engine to connect to the inbuilt
    # sqllite database
    engine = create_engine('mysql+pymysql://root:yoga1234$@localhost/youtubeapi')
    # Write data into the table in sqllite database


    mcd.to_sql('channel_details', engine,if_exists='append',index=False)
    mpd.to_sql('playlist_details', engine,if_exists='append',index=False)
    mvd.to_sql('video_details', engine,if_exists='append',index=False)
    mcod.to_sql('comment_details', engine,if_exists='append',index=False)


def query(input):
    
    match input:
        case "1. what are the names of all the videos and their corresponding channels?":
            query1=("""SELECT c.channel_name,v.videoTitle
            from channel_details c
            join video_details v
            ON c.channel_id = v.channel_id
            ORDER BY c.channel_name;""")
            cursor.execute(query1)
            rows = cursor.fetchall()
            return pd.DataFrame(rows,columns=['channel_name','videotitle'])
            

        case "2. which channels have the most numder of videos, and how many videos do they have?":
            query2=("""SELECT channel_name,videocount
            FROM channel_details
            ORDER BY videocount DESC
            LIMIT 1;""")
            cursor.execute(query2)
            rows = cursor.fetchall()
            return pd.DataFrame(rows,columns=['channel_name','videocount'])
            
            
        case "3. what are the top 10 most viewed videos and their respective channels?":
            query3=("""SELECT c.channel_name,v.videotitle,v.viewcount
            FROM video_details v
            join channel_details c
            ON v.channel_id = c. channel_id
            ORDER BY v.viewcount DESC
            LIMIT 10;""")
            cursor.execute(query3)
            rows = cursor.fetchall()
            return pd.DataFrame(rows,columns=['channel_name','videotitle','viewcount'])
            
            
        case "4. how many comments were  made on each video,and what are their corresponding video names?":
            query4=("""SELECT comment_count,videotitle
            from video_details;""")
            cursor.execute(query4)
            rows = cursor.fetchall()
            return pd.DataFrame(rows,columns=['comment_count','vedioTitle'])
            
        case "5. which videos have the highest number of likes,and what are their corresponding channel names?":
            query5=("""SELECT v.likecount,c.channel_name
            FROM video_details v
            LEFT JOIN channel_details c
            ON v.channel_id = c.channel_id
            ORDER BY likecount desc
            limit 1;""")
            cursor.execute(query5)
            rows = cursor.fetchall()
            return pd.DataFrame(rows,columns=['likecount','videotitle','channel_name'])
            
        case "6. what is the total numder of likes for each video,and what are their corresponding video names?":
            query6=("""SELECT likecount,videotitle
            FROM video_details;""")
            cursor.execute(query6)
            rows = cursor.fetchall()
            return pd.DataFrame(rows,columns=['likecount','vedioTitle'])
        
        case "7. what is the total number of views for each channel,and what are their corresponding channel names?":
            query7=("""SELECT viewcount,channel_name
            FROM channel_details
            order by viewcount desc;""")
            cursor.execute(query7)
            rows = cursor.fetchall()
            return pd.DataFrame(rows,columns=['viewcount','channel_name'])
            
            
        case "8. what are the names of all the channels that have published videos in the year 2022?":
            query8=("""SELECT channel_name
            FROM channel_details
            where year(published_at)='2022';""")
            cursor.execute(query8)
            rows = cursor.fetchall()
            return pd.DataFrame(rows,columns=['viewcount','channel_name'])
             

        case "9. what is the average duration of all videos in each channel,and what are their corresponding channel names?":
            query9=(""" SELECT c.channel_name,avg(v.duration) as average_duration_in_seconds
            from video_details v
            join channel_details c
            on v.channel_id = c.channel_id
            group by c.channel_name;""")
            cursor.execute(query9)
            rows = cursor.fetchall()
            return pd.DataFrame(rows,columns=['channel_name','average_duration_in_seconds']) 
            
        
        case "10. which videos have the highest number of comments,and what are their corresponding channel names?":
            query10=("""SELECT v.videotitle,max(comment_count),c.channel_name  
            from video_details v
            join channel_details c
            ON v.channel_id = c.channel_id
            group by videotitle,c.channel_name
            order by 2 desc;""") 
            cursor.execute(query10)
            rows = cursor.fetchall()
            return pd.DataFrame(rows,columns=['videotitle','comment_count','channel_name'])
            
  