
from own_database import *
import streamlit as st
import pandas as pd

st.title("Youtube Channel detailes")
channel_id = st.text_input("# Enter youtube channel id")
client = MongoClient('mongodb+srv://yogavignesh:user123@cluster0.ge0ozs7.mongodb.net/?ssl=true&ssl_cert_reqs=CERT_NONE')
api_key = 'AIzaSyDqSdf8JteKpRqg-iXpEkMn88jffLQc-Zs'
youtube = build('youtube', 'v3', developerKey=api_key)

if st.button("Get data"):
    channel_out = get_channel_data(youtube, channel_id)
    st.write("#### Channel name")
    st.write("# " + channel_out['channel_name'])

    st.write("#### subscribers")
    st.write("# " + channel_out['subscribers'])

    st.write("#### views")
    st.write("# " + channel_out['views'])

    st.write("#### Total_videos")
    st.write("# " + channel_out['Total_videos'])

    video_list = get_video_ids(youtube, channel_out['playlist_id'])
    database = data_upload_video(client, youtube, channel_out, video_list)
    data_upload_comment(database, youtube, video_list)
    db = get_data(database)
    df = pd.DataFrame(db)
    st.write(df)


