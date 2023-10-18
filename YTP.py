import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd

import googleapiclient.discovery
from pprint import pprint
import re
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
uri = "mongodb+srv://meetmanoj:meetmanoj@cluster0.zdkwf9g.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection
#client=MongoClient( "mongodb+srv://meetmanoj:meetmanoj@cluster0.zdkwf9g.mongodb.net/?retryWrites=true&w=majority")
db=client.youtubedata
coll=db.youtubedetails

API_KEY='AIzaSyBhGFGXGoxFQ6rgdcsnBBkH8kt9UDy1c80'
channel_id="UCPN5EwIY-RR6IR3lZ4DsMbg"
youtube= googleapiclient.discovery.build('youtube', 'v3', developerKey=API_KEY)

import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="", 
)


mycursor = mydb.cursor(buffered=True)

def channel_stats(channel_id):
    ch=[]
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()

    data=dict(channel_name=response['items'][0]['snippet']['title'],
                channel_id=response['items'][0]['id'],
                total_videos=response['items'][0]['statistics']['videoCount'],
                subscribers=response['items'][0]['statistics']['subscriberCount'],
                views=response['items'][0]['statistics']['viewCount'],
                joined_on=response['items'][0]['snippet']['publishedAt']
            )
    ch.append(data)          
    return ch
    
def playlist_details(channel_id):

    playlist_ids=[]
    request=youtube.playlists().list(part="snippet,contentDetails",id=channel_id)
    response=request.execute()
    next_page=None
    
    while True:
        request=youtube.playlists().list(part="snippet,contentDetails",
                                            channelId=channel_id,
                                            maxResults=20,
                                            pageToken=next_page)
        response=request.execute()
        for i in response['items']:
            play_data=dict(Playlistid=i['id'],
                            channel_id=i['snippet']['channelId'],
                            Playlist_name=i['snippet']['title'],
                            Playlist_Description=i['snippet'].get('description')
                            )

            playlist_ids.append(play_data)
        next_page_token=response.get('nextPageToken ')
        if next_page_token is None:
            break
    return playlist_ids
    
def channel_videos(channel_id):
    video_ids=[]
    video_response=youtube.channels().list(id=channel_id,part='contentDetails').execute()
    playlist_id=video_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None

    while True:
        response=youtube.playlistItems().list(playlistId=playlist_id,part='snippet',maxResults=50,pageToken=next_page_token).execute()

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids
    
def convert_duration(duration):
            regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
            match = re.match(regex, duration)
            if not match:
                return '00:00:00'
            hours, minutes, seconds = match.groups()
            hours = int(hours[:-1]) if hours else 0
            minutes = int(minutes[:-1]) if minutes else 0
            seconds = int(seconds[:-1]) if seconds else 0
            total_seconds = hours * 3600 + minutes * 60 + seconds
            return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60))
    
def video_details(videoids):
    video_stats=[]

    for i in range(0,len(videoids),50):
        response=youtube.videos().list(part='snippet,contentDetails,statistics',id=','.join(videoids[i:i+50])).execute()

        for video in response['items']:
            video_details=dict(channel_name=video['snippet']['channelTitle'],
                                channel_id=video['snippet']['channelId'],
                                video_id=video['id'],
                                video_name=video['snippet']['title'],
                                video_description=video['snippet']['description'],
                                duration=convert_duration(video['contentDetails']['duration']),
                                view_count=video['statistics']['viewCount'],
                                likes_count=video['statistics'].get('likeCount',0),
                                dislikes_count=video['statistics'].get('dislikeCount',0),
                                definition=video['contentDetails']['definition'],
                                favorite_count=video['statistics']['favoriteCount'],
                                comment_count=video['statistics'].get('commentCount'),
                                publishedat=video['snippet'].get('publishedAt')
                                )
            video_stats.append(video_details)
    return   video_stats
    
def comment_details(videoids):
    comment_stats=[]
    for i in videoids:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=i)

            response = request.execute()
            for cmnt in response['items']:
                comment_data=dict(channel_id=cmnt['snippet']['topLevelComment']['snippet']['channelId'],
                                    video_id=cmnt['snippet']['topLevelComment']['snippet']['videoId'],
                                    authorname=cmnt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    publishedAT=cmnt['snippet']['topLevelComment']['snippet']['publishedAt'],
                                    updated=cmnt['snippet']['topLevelComment']['snippet']['updatedAt'],
                                    Likes=cmnt['snippet']['topLevelComment']['snippet']['likeCount'])
                comment_stats.append(comment_data)
        except:
            pass

    return comment_stats
    
def details(channel_id):
    c=channel_stats(channel_id)
    p=playlist_details(channel_id)
    videoids=channel_videos(channel_id)
    vd=video_details(videoids)
    cm=comment_details(videoids)
    data={'channel':c,
        'playlist':p,
        'videolist':vd,
        'comment details':cm}
    return data

# def details(channel_id):
#     c = channel_stats(channel_id)
#     if c:
#         channel_data = c[0]  
#         p = playlist_details(channel_data["channel_id"])
#         videoids = channel_videos(channel_id)
#         vd = video_details(videoids)
#         cm = comment_details(videoids)
#         data = {
#             'channel': c,
#             'playlist': p,
#             'videolist': vd,
#             'comment details': cm
#         }
#         return data
#     else:
#         return None  

def channel_name():
    
    ch_name = [document['channel'][0]['channel_name'] for document in db.youtubedetails.find({},{'channel':1})]
    return ch_name

 

mycursor.execute("CREATE DATABASE if not exists youtubedetails")

mycursor.execute("USE  youtubedetails")  

mycursor.execute('''CREATE TABLE if not exists channeldata (channel_name VARCHAR(255),
                                        channel_id VARCHAR(255),
                                        total_videos VARCHAR(255),
                                        subscribers VARCHAR(255),
                                        views VARCHAR(255),
                                        joined_on DATETIME)''')
mydb.commit()

mycursor.execute('''CREATE TABLE if not exists playlistdata (Playlistid VARCHAR(255),
                                        channel_id VARCHAR(255),
                                        Playlist_name TEXT,
                                        Playlist_Description TEXT)''')
                                        
mydb.commit()

mycursor.execute('''CREATE TABLE if not exists videodata (channel_name VARCHAR(255),
                                        channel_id VARCHAR(255),
                                        video_id VARCHAR(255),
                                        video_name VARCHAR(255),
                                        video_description TEXT,                         
                                        duration TIME,
                                        view_count INT,
                                        likes_count INT,
                                        dislikes_count INT,
                                        definition VARCHAR(255),
                                        favorite_count INT,
                                        comment_count INT,
                                        publishedat DATETIME)''')
                                        
mydb.commit()

mycursor.execute('''CREATE TABLE if not exists commentdata  (channel_id VARCHAR(255),
                                            video_id VARCHAR(255),
                                            authorname VARCHAR(255),
                                            publishedAT DATETIME,
                                            updated DATETIME,
                                            Likes int)''')
mydb.commit()

def sql_main(md):
    sql_c='''INSERT INTO channeldata(channel_name,channel_id,total_videos,subscribers,views,joined_on )values(%s,%s,%s,%s,%s,%s)'''
    val=tuple(md['channel'][0].values())
    mycursor.execute(sql_c,val)

    sql_p ='''INSERT INTO playlistdata(Playlistid,channel_id,Playlist_name,Playlist_Description)values(%s,%s,%s,%s)'''
    for i in md['playlist']:
        val=tuple(i.values())
        mycursor.execute(sql_p,val)

    sql_v ='''INSERT INTO videodata(channel_name,channel_id,video_id,video_name,video_description,duration,view_count,likes_count,dislikes_count,definition,favorite_count,comment_count,publishedat)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    for j in md['videolist']:
        val=tuple(j.values())
        mycursor.execute(sql_v,val)


    sql_cm ='''INSERT INTO commentdata(channel_id,video_id,authorname,publishedAT,updated,Likes)values(%s,%s,%s,%s,%s,%s)'''
    for k in md['comment details']:
        val=tuple(k.values())
        mycursor.execute(sql_cm,val)
    mydb.commit()


def display_data_collection():
    data = {
        "Channel Names": ["Meenakshi Hospital", "FORMAL INFINITY", "Matthew Anderson","Shabarinath Premlal","Bonbon Creato","BeamNG A","COAS TOYS","Visibility Track","Mettupalayam Engineer","Horticulture 05"],
        "Channel Id": ['UCbadTGW6vuPYkcQj3T0AAzA', 'UCPN5EwIY-RR6IR3lZ4DsMbg', 'UCFTEN8q_sQNHVENU4KcCMAA','UCQqmjKQBKQkRbWbSntYJX0Q','UCljnPgs9KaOlhV-TwecOh8g','UCBTOWy0kYezPVZxSdJRXJ3g','UCO49SW9nK1Npc3R4WnW52cA','UC1TpCbmg6ckk-yi7Vd95akQ','UCmuvOB77oQEqXA9xMPniTsw','UCC9d_pYCYYJUES-Gq4Q3g8A']
    }
    df = pd.DataFrame(data)
    
    st.dataframe(df)

st.set_page_config(page_title="Youtube Data Harvesting And Warehousing", layout='wide', initial_sidebar_state="expanded")
selected = st.sidebar.selectbox("Choose an option", ["Data Collection", "Get Data", "Migrate Zone", "Data Analysis","ABOUT"])

if selected == "Data Collection":
    display_data_collection()

if selected == "Get Data":    

    st.write("### Enter YouTube Channel_ID below :")
    channel_id = st.text_input('**Enter channel_ID**')

    if channel_id and st.button("Extract Data"):
        c=channel_stats(channel_id)
        playlistid=playlist_details(channel_id)
        videoids=channel_videos(channel_id)
        vd=video_details(videoids)
        cmnt_Details=comment_details(videoids)
        st.write(c)

    if st.button("upload to Mongodb"): 
       md=details(channel_id)
       st.write(md)
       coll.insert_one(md)
       st.success("Upload to Mongodb successful !!")
    

if selected == "Migrate Zone":
    

    st.header(':blue[Migration of Data]')
    st.markdown("# ")
    st.markdown("select a channel to begin transformation to SQL")

    ch=channel_name()

    user_inp=st.selectbox("select channel", ch)
     

     

    if st.button("upload to SQL"):
        md=coll.find_one({"channel.channel_name": user_inp})
        st.write(md)
        sql_main(md)
        st.success("Upload to SQL successfully !!")
    

if selected == "Data Analysis":     
   
    
    st.write("# :orange[Select any Query]")
    questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':

        mycursor.execute("SELECT video_name,channel_name FROM videodata ORDER BY channel_name ")
        out = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(out)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
    
        mycursor.execute("SELECT  channel_name,total_videos FROM channeldata ORDER BY total_videos DESC ")
        out = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(out)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
    
        mycursor.execute("SELECT video_name,channel_name,view_count  FROM videodata   ORDER BY view_count DESC limit 10 ")
        out = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(out)

        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        
        mycursor.execute("SELECT video_name,channel_name,comment_count  FROM videodata ORDER BY comment_count DESC ")
        out = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(out)

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    
        mycursor.execute("SELECT video_name,channel_name,likes_count  FROM videodata   ORDER BY likes_count DESC ")
        out = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(out)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    
        mycursor.execute("SELECT video_name,channel_name,likes_count,dislikes_count  FROM videodata GROUP BY video_name ORDER BY dislikes_count  AND likes_count DESC ")
        out = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(out)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    
        mycursor.execute("SELECT channel_name, views  FROM channeldata   ORDER BY views DESC  ")
        out = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(out)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
    
        mycursor.execute("SELECT  channel_name,publishedat FROM videodata  WHERE YEAR(publishedat) ='2022'  ")
        out = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(out)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    
        mycursor.execute("SELECT  channel_name,AVG(duration)  FROM videodata GROUP BY 1  ")
        out = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(out)

        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        
        mycursor.execute("SELECT channel_name, video_name,comment_count  FROM Videodata   ORDER BY comment_count DESC  ")
        out = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(out)

    pass




if selected=="ABOUT":
    st.title('YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit')
    st.subheader('Problem Statement')
    st.caption('The task is to build a Streamlit app that permits users to analyze data from multiple YouTube channels.' 
               'Users can input a YouTube channel ID to access data like channel information, video details, and user engagement.' 
               'The app should facilitate storing the data in a MongoDB database and allow users to collect data from up to 10 different channels.' 
               'Additionally, it should offer the capability to migrate selected channel data from the data lake to a SQL database for further analysis.' 
               'The app should enable searching and retrieval of data from the SQL database, including advanced options like joining tables for comprehensive channel information.')
   
    st.subheader('Technology Stack Used')

    st.text('1.Python.')
    st.text('2.MONGO DB.')
    st.text('3.MY SQL.')
    st.text('4.google api client Library.')
    st.text('5.Pandas Library.')
    st.text('6.Streamlit.') 
    
    st.subheader('Approach')

    st.caption('1. Start by setting up a Streamlit application using the python library "streamlit", which provides an easy-to-use interface for users to enter a YouTube channel ID, view channel details, and select channels to migrate.')
    st.caption('2. Establish a connection to the YouTube API V3, which allows me to retrieve channel and video data by utilizing the Google API client library for Python.')
    st.caption('3. Store the retrieved data in a MongoDB data base, as MongoDB is a suitable choice for handling unstructured and semi-structured data. This is done by firstly writing a method to retrieve the previously called api call and storing the same data in the database in 10 different collections.')           
    st.caption('4. Transferring the collected data from multiple channels namely the channels,videos and comments to a SQL data warehouse, utilizing a SQL database like MySQL or PostgreSQL for this purpose.')           
    st.caption('5. Utilize SQL queries to join tables within the SQL data warehouse and retrieve specific channel data based on user input. For that the SQL table previously made has to be properly given the query.')           
    st.caption('6. The retrieved data is displayed within the Streamlit application, leveraging Streamlit users to analyze the data.')          










    