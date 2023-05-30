from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
def get_channel_data(youtube,channel_id):
    request = youtube.channels().list(
            part='snippet,contentDetails,statistics',
            id=channel_id)
    response = request.execute()
    data = dict(channel_name=response['items'][0]['snippet']['title'],
                        subscribers=response['items'][0]['statistics']['subscriberCount'],
                        views=response['items'][0]['statistics']['viewCount'],
                        Total_videos=response['items'][0]['statistics']['videoCount'],
                        playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    return data
def get_video_ids(youtube, playlist_id):
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id,
        maxResults=50)
    response = request.execute()
    video_ids = []
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
    next_page_token = response.get('nextPageToken')
    more_pages = True

    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])

            next_page_token = response.get('nextPageToken')

    return video_ids

def get_video_details(youtube, video_ids):
    request = youtube.videos().list(
            part='snippet,statistics',
            id=video_ids)
    response = request.execute()
    video_stats = dict(Title=response['items'][0]['snippet']['title'],
                        Published_date=response['items'][0]['snippet']['publishedAt'],
                        Views=response['items'][0]['statistics']['viewCount'],
                        Likes=response['items'][0]['statistics']['likeCount'],
                        favoriteCount=response['items'][0]['statistics']['favoriteCount'],
                        Comments=response['items'][0]['statistics']['commentCount'],
                        )
    return video_stats


def get_video_comments(youtube, video_id):
    comments = []
    nextPageToken = None

    while True:
        response = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            pageToken=nextPageToken,
            maxResults=100
        ).execute()

        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            comments.append(comment)

        nextPageToken = response.get('nextPageToken')

        if not nextPageToken:
            break

    return comments
def get_video_comments(youtube, video_id):
    comments = []
    nextPageToken = None

    try:
        while True:
            response = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                pageToken=nextPageToken,
                maxResults=100
            ).execute()

            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                comments.append(comment)

            nextPageToken = response.get('nextPageToken')

            if not nextPageToken:
                break

    except HttpError as e:
        error_msg = e.content.decode("utf-8")
        error_reason = e.resp.reason
        error_details = e._get_reason() if hasattr(e, '_get_reason') else None
        print(f"An error occurred while retrieving comments: {error_msg}")
        print(f"Reason: {error_reason}")
        print(f"Details: {error_details}")

    return comments