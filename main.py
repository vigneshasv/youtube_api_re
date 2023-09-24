import streamlit as st
from source import *
# Using object notation



api_key='AIzaSyCIRXDBtfFgCbEXNVfxTMRH9UIvx92Weqo'
st.title('youtube data harvesting')     



# Using "with" notation
with st.sidebar:
    add_selectbox = st.selectbox(
    "Select mode",
    ("Get data", "Migrate data", "Query")
    )
    
    if add_selectbox is "Get data":
        ch_id = st.text_input('Channel ID')
        gu = st.button("get data", type="primary")
        
    if add_selectbox is "Migrate data":
        mu = st.button("Migrate", type="primary")
    
    if add_selectbox is "Query":
        select_query = st.selectbox(
        "Select your Query?",
        ("1. what are the names of all the videos and their corresponding channels?",
         "2. which channels have the most numder of videos, and how many videos do they have?",
         "3. what are the top 10 most viewed videos and their respective channels?",
         "4. how many comments were  made on each video,and what are their corresponding video names?",
         "5. which videos have the highest number of likes,and what are their corresponding channel names?",
         "6. what is the total numder of likes for each video,and what are their corresponding video names?",
         "7. what is the total number of views for each channel,and what are their corresponding channel names?",
         "8. what are the names of all the channels that have published videos in the year 2022?",
         "9. what is the average duration of all videos in each channel,and what are their corresponding channel names?",
         "10. which videos have the highest number of comments,and what are their corresponding channel names?")
        )
    
        qu = st.button("get query..", type="primary")
    
if add_selectbox is "Get data" and gu:
    st.write('Data getting....') 
    get_data(ch_id ,api_key ) 
    st.write('Data loaded') 
    
elif add_selectbox is "Migrate data" and mu:
    st.title('Migrate....') 
    migrate_data()
    st.title('Migrate finish') 
 
elif add_selectbox is "Query" and qu:
    st.title('Loading Query') 
    df = query(select_query)
    st.title('loaded') 
    st.dataframe(df)
    

    