from youtube_api import *
from pymongo import MongoClient
def data_upload_video(client, youtube, database, videoid):
    db_name = database['channel_name']
    db = client[db_name.replace(' ', '_')]
    channel_clo = db['channel_data']
    channel_clo.insert_one(database)
    video_clo = db['video_list']
    for i in videoid:
        # comment = get_video_comments(youtube, i)
        data = get_video_details(youtube, i)
        data['video_id'] = i
        video_clo.insert_one(data)
    return db

def data_upload_comment(database, youtube, videoid):
    col = database['comments']
    for i in videoid:
        comment = get_video_comments(youtube, i)
        data = {}
        data['comment'] = comment
        data['video_id'] = i
        col.insert_one(data)

def get_data(database):
    data = []
    for i in database.video_list.find():
        data.append(i)
    return data

def channel_names(data, title):
    data_n = [i.get(title) for i in data]
    return data_n

