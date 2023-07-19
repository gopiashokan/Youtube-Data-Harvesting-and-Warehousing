import googleapiclient.discovery
import pandas as pd
import psycopg2
import pymongo
import streamlit as st
import plotly.express as px
import datetime

st.set_page_config(page_title='YouTube Data Harvesting and Warehousing', page_icon=':bar_chart:', layout="wide")
pd.set_option('display.max_columns', None)


# --------------Data Retrieving from youtube ---------------

# channel data retrive from youtube
def get_channel_details(youtube, channel_id):
    request = youtube.channels().list(
        part='contentDetails, snippet, statistics, status',
        id=channel_id)
    response = request.execute()

    data = {'channel_name': response['items'][0]['snippet']['title'],
            'channel_id': response['items'][0]['id'],
            'subscription_count': response['items'][0]['statistics']['subscriberCount'],
            'channel_views': response['items'][0]['statistics']['viewCount'],
            'channel_description': response['items'][0]['snippet']['description'],
            'upload_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            'country': response['items'][0]['snippet']['country']}
    return data


# playlist data retrive from youtube
def get_total_playlists(youtube, channel_id, upload_id):
    request = youtube.playlists().list(
        part="snippet,contentDetails,status",
        channelId=channel_id,
        maxResults=50)
    response = request.execute()

    playlists = {}
    p = 1

    for i in range(0, len(response['items'])):
        data = {'playlist_id': response['items'][i]['id'],
                'playlist_name': response['items'][i]['snippet']['title'],
                'channel_id': channel_id,
                'upload_id': upload_id}
        p1 = 'playlist_id_' + str(p)
        playlists[p1] = data
        p += 1
    next_page_token = response.get('nextPageToken')

    umbrella = True
    while umbrella:
        if next_page_token is None:
            umbrella = False
        else:
            request = youtube.playlists().list(
                part="snippet,contentDetails,status",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            for i in range(0, len(response['items'])):
                data = {'playlist_id': response['items'][i]['id'],
                        'playlist_name': response['items'][i]['snippet']['title'],
                        'channel_id': channel_id,
                        'upload_id': upload_id}
                p1 = 'playlist_id_' + str(p)
                playlists[p1] = data
                p += 1
            next_page_token = response.get('nextPageToken')

    return playlists


# video ids retrive from youtube
def get_total_video_ids(youtube, upload_id):
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=upload_id,
        maxResults=50)
    response = request.execute()

    list_video_ids = []

    for i in range(0, len(response['items'])):
        data = response['items'][i]['contentDetails']['videoId']
        list_video_ids.append(data)
    next_page_token = response.get('nextPageToken')

    umbrella = True
    while umbrella:
        if next_page_token is None:
            umbrella = False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=upload_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            for i in range(0, len(response['items'])):
                data = response['items'][i]['contentDetails']['videoId']
                list_video_ids.append(data)
            next_page_token = response.get('nextPageToken')

    return list_video_ids


# video details retrive from youtube
def get_video_details(youtube, video_id, upload_id):
    request = youtube.videos().list(
        part='contentDetails, snippet, statistics',
        id=video_id)
    response = request.execute()

    cap = {'true': 'Available', 'false': 'Not Available'}

    def time_duration(t):
        a = pd.Timedelta(t)
        b = str(a).split()[-1]
        return b

    data = {'video_id': response['items'][0]['id'],
            'video_name': response['items'][0]['snippet']['title'],
            'video_description': response['items'][0]['snippet']['description'],
            'upload_id': upload_id,
            'tags': response['items'][0]['snippet'].get('tags', []),
            'published_date': response['items'][0]['snippet']['publishedAt'][0:10],
            'published_time': response['items'][0]['snippet']['publishedAt'][11:19],
            'view_count': response['items'][0]['statistics']['viewCount'],
            'like_count': response['items'][0]['statistics'].get('likeCount', 0),
            'favourite_count': response['items'][0]['statistics']['favoriteCount'],
            'comment_count': response['items'][0]['statistics'].get('commentCount', 0),
            'duration': time_duration(response['items'][0]['contentDetails']['duration']),
            'thumbnail': response['items'][0]['snippet']['thumbnails']['default']['url'],
            'caption_status': cap[response['items'][0]['contentDetails']['caption']]}
    if data['tags'] == []:
        del data['tags']

    return data


# comment details retrive from youtube
def get_comments_details(youtube, video_id):
    request = youtube.commentThreads().list(
        part='id, snippet',
        videoId=video_id,
        maxResults=100)
    response = request.execute()

    list_comments = {}
    c = 1

    for i in range(0, len(response['items'])):
        data = {'comment_id': response['items'][i]['id'],
                'comment_text': response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                'comment_author': response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'comment_published_date': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][
                                          0:10],
                'comment_published_time': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][
                                          11:19],
                'video_id': video_id}
        c1 = 'comment_id_' + str(c)
        list_comments[c1] = data
        c += 1
    return list_comments


# combine the channel, playlists, videos & comments data into single dictionary file
def data_extraction_youtube(channel_id):
    channel_data = get_channel_details(youtube, channel_id)
    upload_id = channel_data['upload_id']

    playlist_data = get_total_playlists(youtube, channel_id, upload_id)
    total_video_ids = get_total_video_ids(youtube, upload_id)

    merge = {}
    video_with_comments = {}
    v = 1
    for i in total_video_ids:
        video_data = get_video_details(youtube, i, upload_id)
        merge.update(video_data)
        if int(video_data['comment_count']) > 0:
            comments_data = get_comments_details(youtube, i)
            merge['comments'] = comments_data
        v1 = 'video_id_' + str(v)
        video_with_comments[v1] = merge
        v += 1
        merge = {}

    final = {'channel_name': channel_data, 'playlists': playlist_data}
    final.update(video_with_comments)
    return final


# display sample json file in streamlit page
def display_sample_data(channel_id):
    channel_data1 = get_channel_details(youtube, channel_id)
    upload_id1 = channel_data1['upload_id']

    playlist_data1 = get_total_playlists(youtube, channel_id, upload_id1)
    total_video_ids1 = get_total_video_ids(youtube, upload_id1)

    merge1 = {}
    video_with_comments1 = {}
    for i in total_video_ids1:
        video_data1 = get_video_details(youtube, i, upload_id1)
        merge1.update(video_data1)
        if int(video_data1['comment_count']) > 0:
            comments_data1 = get_comments_details(youtube, i)
            merge1['comments'] = comments_data1
        video_with_comments1['video_id_1'] = merge1
        break

    final1 = {'channel_name': channel_data1, 'playlists': playlist_data1}
    final1.update(video_with_comments1)
    st.subheader('Sample Output Data:')
    return final1


# ---------------- Data save to MongoDB database ----------------------

# list of all collections in the 'project_youtube' database in MongoDB
def list_mongodb_collection_names(database):
    gopi = pymongo.MongoClient(
        "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
    db = gopi[database]
    col = db.list_collection_names()
    col.sort(reverse=False)
    return col


# collection names in order wise
def order_mongodb_collection_names():
    st.subheader('List of collections in MongoDB database')
    c = 1
    m = list_mongodb_collection_names(database)
    for i in m:
        st.write(str(c) + ' - ' + i)
        c += 1


# retrive data store to MongoDB database
def data_store_mongodb(channel_name, database, data_youtube):
    gopi = pymongo.MongoClient(
        "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
    db = gopi[database]
    col = db[channel_name]
    col.insert_one(data_youtube)


# temporary database to store retrive data and finally automatically drop
def temp_collection_drop():
    gopi = pymongo.MongoClient(
        "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
    db = gopi['temp']
    col = db.list_collection_names()
    if len(col) > 0:
        for i in col:
            db.drop_collection(i)


def mongodb(database):
    data_youtube = {}
    gopi = pymongo.MongoClient(
        "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
    db = gopi['temp']
    col = db.list_collection_names()
    channel_name = col[0]

    # Now we get the channel name
    col1 = db[channel_name]
    for i in col1.find():
        data_youtube.update(i)

    list_collections_name = list_mongodb_collection_names(database)

    if channel_name not in list_collections_name:
        data_store_mongodb(channel_name, database, data_youtube)
        st.success("The data has been successfully stored in the MongoDB database")
        st.balloons()
        temp_collection_drop()
    else:
        st.warning("The data has already been stored in MongoDB database")
        option = st.radio('Do you want to overwrite the data currently stored?',
                          ['Select the option below', 'Yes', 'No'])
        if option == 'Yes':
            gopi = pymongo.MongoClient(
                "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
            db = gopi[database]
            db[channel_name].drop()
            data_store_mongodb(channel_name, database, data_youtube)
            st.success("The data has been successfully overwritten and updated in MongoDB database")
            st.balloons()
            temp_collection_drop()
        elif option == 'No':
            temp_collection_drop()
            st.info("The data overwrite process has been skipped")


# ---------------- Migrating to SQL Database -----------------------

# Create Database and Table in SQL
def sql_create_tables():
    gopi_sql = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_sql.cursor()
    cursor.execute("create table if not exists channel(\
                        channel_id 			varchar(255) primary key,\
                        channel_name		varchar(255),\
                        subscription_count	int,\
                        channel_views		int,\
                        channel_description	text,\
                        upload_id			varchar(255),\
                        country				varchar(255))")

    cursor.execute("create table if not exists playlist(\
                        playlist_id		varchar(255) primary key,\
                        playlist_name	varchar(255),\
                        channel_id		varchar(255),\
                        upload_id		varchar(255))")

    cursor.execute("create table if not exists video(\
                        video_id			varchar(255) primary key,\
                        video_name			varchar(255),\
                        video_description	text,\
                        upload_id			varchar(255),\
                        tags				text,\
                        published_date		date,\
                        published_time		time,\
                        view_count			int,\
                        like_count			int,\
                        favourite_count		int,\
                        comment_count		int,\
                        duration			time,\
                        thumbnail			varchar(255),\
                        caption_status		varchar(255))")

    cursor.execute("create table if not exists comment(\
                        comment_id				varchar(255) primary key,\
                        comment_text			text,\
                        comment_author			varchar(255),\
                        comment_published_date	date,\
                        comment_published_time	time,\
                        video_id				varchar(255))")

    gopi_sql.commit()

# SQL channel names list
def list_sql_channel_names():
    gopi = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi.cursor()
    cursor.execute("select channel_name from channel")
    s = cursor.fetchall()
    s = [i[0] for i in s]
    s.sort(reverse=False)
    return s


# display the all channel names from SQL channel table
def order_sql_channel_names():
    s = list_sql_channel_names()
    if s == []:
        st.info("The SQL database is currently empty")
    else:
        st.subheader("List of channels in SQL database")
        c = 1
        for i in s:
            st.write(str(c) + ' - ' + i)
            c += 1


# data migrating to channel table
def sql_channel(database, col_input):
    gopi_mdb = pymongo.MongoClient(
        "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
    db = gopi_mdb[database]
    col = db[col_input]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    data = []
    for i in col.find({}, {'_id': 0, 'channel_name': 1}):
        data.append(i['channel_name'])

    channel = pd.DataFrame(data)
    channel = channel.reindex(columns=['channel_id', 'channel_name', 'subscription_count', 'channel_views',
                                       'channel_description', 'upload_id', 'country'])
    channel['subscription_count'] = pd.to_numeric(channel['subscription_count'])
    channel['channel_views'] = pd.to_numeric(channel['channel_views'])
    return channel


# data migrating to playlist table
def sql_playlists(database, col_input):
    gopi_mdb = pymongo.MongoClient(
        "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
    db = gopi_mdb[database]
    col = db[col_input]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    data = []
    for i in col.find({}, {'_id': 0, 'playlists': 1}):
        data.append(i['playlists'].values())

    playlists = pd.DataFrame(data[0])
    playlists = playlists.reindex(columns=['playlist_id', 'playlist_name', 'channel_id', 'upload_id'])
    return playlists


# data migrating to video table
def sql_videos(database, col_input):
    gopi_mdb = pymongo.MongoClient(
        "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
    db = gopi_mdb[database]
    col = db[col_input]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    data = []
    for i in col.find({}, {'_id': 0, 'channel_name': 0, 'playlists': 0}):
        data.append(i.values())

    videos = pd.DataFrame(data[0])
    videos = videos.reindex(
        columns=['video_id', 'video_name', 'video_description', 'upload_id', 'tags', 'published_date', 'published_time',
                 'view_count', 'like_count', 'favourite_count', 'comment_count', 'duration', 'thumbnail',
                 'caption_status', 'comments'])

    videos['published_date'] = pd.to_datetime(videos['published_date']).dt.date
    videos['published_time'] = pd.to_datetime(videos['published_time'], format='%H:%M:%S').dt.time
    videos['view_count'] = pd.to_numeric(videos['view_count'])
    videos['like_count'] = pd.to_numeric(videos['like_count'])
    videos['favourite_count'] = pd.to_numeric(videos['favourite_count'])
    videos['comment_count'] = pd.to_numeric(videos['comment_count'])
    videos['duration'] = pd.to_datetime(videos['duration'], format='%H:%M:%S').dt.time
    videos.drop(columns='comments', inplace=True)
    return videos


# data migrating to comment table
def sql_comments(database, col_input):
    gopi_mdb = pymongo.MongoClient(
        "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
    db = gopi_mdb[database]
    col = db[col_input]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    data_videos = []
    for i in col.find({}, {'_id': 0, 'channel_name': 0, 'playlists': 0}):
        data_videos.append(i.values())

    videos = pd.DataFrame(data_videos[0])
    videos = videos.reindex(
        columns=['video_id', 'video_name', 'video_description', 'upload_id', 'tags', 'published_date', 'published_time',
                 'view_count', 'like_count', 'favourite_count', 'comment_count', 'duration', 'thumbnail',
                 'caption_status', 'comments'])

    videos['published_date'] = pd.to_datetime(videos['published_date']).dt.date
    videos['published_time'] = pd.to_datetime(videos['published_time'], format='%H:%M:%S').dt.time
    videos['view_count'] = pd.to_numeric(videos['view_count'])
    videos['like_count'] = pd.to_numeric(videos['like_count'])
    videos['favourite_count'] = pd.to_numeric(videos['favourite_count'])
    videos['comment_count'] = pd.to_numeric(videos['comment_count'])
    videos['duration'] = pd.to_datetime(videos['duration'], format='%H:%M:%S').dt.time

    data = []
    for i in videos['comments'].tolist():
        if isinstance(i, dict):
            data.extend(list(i.values()))
        else:
            pass

    comments = pd.DataFrame(data)
    comments = comments.reindex(columns=['comment_id', 'comment_text', 'comment_author',
                                         'comment_published_date', 'comment_published_time', 'video_id'])
    comments['comment_published_date'] = pd.to_datetime(comments['comment_published_date']).dt.date
    comments['comment_published_time'] = pd.to_datetime(comments['comment_published_time'], format='%H:%M:%S').dt.time
    return comments


def sql(database):
    sql_create_tables()
    order_mongodb_collection_names()
    order_sql_channel_names()

    list_mongodb_notin_sql = ['Select the option']
    m = list_mongodb_collection_names(database)
    s = list_sql_channel_names()
    for i in m:
        if i not in s:
            list_mongodb_notin_sql.append(i)

    option_sql = st.selectbox('', list_mongodb_notin_sql)
    if option_sql:
        if option_sql == 'Select the option':
            st.warning('Please select the channel')
        else:
            col_input = option_sql

            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)

            channel = sql_channel(database, col_input)
            playlists = sql_playlists(database, col_input)
            videos = sql_videos(database, col_input)
            comments = sql_comments(database, col_input)

            gopi_sql = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
            cursor = gopi_sql.cursor()

            cursor.executemany("insert into channel(channel_id, channel_name, subscription_count, channel_views,\
                                                channel_description, upload_id, country) values(%s,%s,%s,%s,%s,%s,%s)",
                               channel.values.tolist())
            cursor.executemany("insert into playlist(playlist_id, playlist_name, channel_id, upload_id)\
                                                values(%s,%s,%s,%s)", playlists.values.tolist())
            cursor.executemany("insert into video(video_id, video_name, video_description, upload_id, tags, published_date,\
                                                published_time, view_count, like_count, favourite_count, comment_count, duration, thumbnail,\
                                                caption_status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                               videos.values.tolist())
            cursor.executemany("insert into comment(comment_id, comment_text, comment_author, comment_published_date,\
                                                comment_published_time, video_id) values(%s,%s,%s,%s,%s,%s)",
                               comments.values.tolist())

            gopi_sql.commit()
            st.success("Migrated Data Successfully to SQL Data Warehouse")
            st.balloons()
            gopi_sql.close()


# ----------------------- Analyse the data --------------------------

# List of all channels
def channels_totalchannels():
    st.subheader('List of Channels')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select channel_name from channel order by channel_name ASC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Channel Names'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# all channel with playlist names
def channel_totalplaylists_channelnames():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select distinct playlist.playlist_name, channel.channel_name\
                    from playlist\
                    inner join channel on playlist.channel_id = channel.channel_id\
                    group by playlist.playlist_name, channel.channel_name\
                    order by channel.channel_name, playlist.playlist_name ASC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Playlist Names', 'Channel Names'], index=i)
    df = df.reindex(columns=['Channel Names', 'Playlist Names'])
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# selected channel with playlist names
def channel_totalplaylists_selectchannelnames(channel):
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute('select distinct playlist.playlist_name, channel.channel_name\
                        from playlist\
                        inner join channel on playlist.channel_id = channel.channel_id\
                        where channel.channel_name=\'' + channel + '\'\
                        group by playlist.playlist_id, channel.channel_name\
                        order by channel.channel_name, playlist.playlist_name ASC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Playlist Names', 'Channel Names'], index=i)
    df = df.reindex(columns=['Channel Names', 'Playlist Names'])
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# channel wise playlists count
def channels_channelnames_totalplaylists():
    st.subheader('Channel wise Playlists')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select distinct channel.channel_name, count(distinct playlist.playlist_id) as total\
                    from playlist\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by channel.channel_id\
                    order by total DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Channel Names', 'Total Playlists'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# channel wise total videos count
def channels_channelnames_totalvideos():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select channel.channel_name, count(distinct video.video_id) as total\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by channel.channel_id\
                    order by total DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Channel Names', 'Total Videos'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# start and end date based total published videos
def channels_channelnames_publishvideos(start_date, end_date):
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute('select distinct channel.channel_name, count(distinct video.video_id) as total\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    where video.published_date between\'' + str(start_date) + '\'and\'' + str(end_date) + '\'\
                    group by channel.channel_id\
                    order by total DESC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Channel Names', 'Published videos'], index=i)
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# channel wise subscriptions
def channels_channelnames_subscriptions():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select channel_name, subscription_count from channel\
                   order by subscription_count DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Channel Names', 'Total Subscriptions'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# channel wise views
def channels_channelnames_views():
    st.subheader('Channel wise  Views')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select channel_name, channel_views from channel\
                   order by channel_views DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Channel Names', 'Total Views'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# channel wise total likes
def channels_channelnames_totallikes():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select distinct channel.channel_name, cast(sum(subquery.sum1) as int) as sum2\
                    from channel\
                    inner join(select distinct playlist.channel_id, video.video_id, sum(distinct video.like_count) as sum1\
                    from playlist\
                    inner join video on playlist.upload_id = video.upload_id\
                    group by playlist.channel_id, video.video_id\
                    )as subquery on subquery.channel_id = channel.channel_id\
                    group by channel.channel_name\
                    order by sum2 DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Channel Names', 'Total Likes'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# channel wise total comments
def channels_channelnames_totalcomments():
    st.subheader('Channel wise  Comments')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select distinct channel.channel_name, count(distinct comment.comment_id) as total\
                    from comment\
                    inner join video on video.video_id = comment.video_id\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by channel.channel_name\
                    order by total DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Channel Names', 'Total Comments'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# convert days with time to HH:MM:SS
def convert_duration(duration):
    days, time = duration.split(' days ')
    hours, minutes, seconds = map(int, time.split(':'))
    total_hours = int(days) * 24 + hours
    formatted_duration = f'{total_hours:02d}:{minutes:02d}:{seconds:02d}'
    return formatted_duration


# channel wise total video durations
def channels_channelnames_totaldurations():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select channel.channel_name, sum(video.duration) as total\
                    from video\
                    inner join channel on channel.upload_id = video.upload_id\
                    group by channel.channel_id\
                    order by total DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Channel Names', 'Total Durations'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    df['Total Durations'] = df['Total Durations'].apply(lambda x: convert_duration(str(x)))
    return df


# channel wise average video durations
def channels_channelnames_avgdurations():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select channel.channel_name, SUBSTRING(CAST(avg(video.duration) AS VARCHAR), 1, 8) as average\
                    from video\
                    inner join channel on channel.upload_id = video.upload_id\
                    group by channel.channel_id\
                    order by average DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Channel Names', 'Average Durations'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# combine all data with charts
def analysis_channels():
    # Channel List and Channel wise Playlist Names
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(channels_totalchannels())
    with col2:
        st.subheader('Channel wise Playlists')
        channel = st.selectbox('', list_channel)
        if channel == 'Over All':
            st.dataframe(channel_totalplaylists_channelnames())
        else:
            st.dataframe(channel_totalplaylists_selectchannelnames(channel))

    # Channel wise Playlist Counts pie
    df = channels_channelnames_totalplaylists()
    df_sorted = df.sort_values(by='Total Playlists', ascending=True)
    col3, col4 = st.columns(2)
    with col3:
        st.dataframe(df)
    with col4:
        fig = px.pie(df_sorted, names='Channel Names', values='Total Playlists', hole=0.5)
        fig.update_traces(text=df_sorted['Channel Names'], textinfo='percent+value', textposition='outside',
                          textfont=dict(color='white'))
        st.plotly_chart(fig, use_container_width=True)

    # Channel wise Videos bar
    st.subheader('Channel wise Videos')
    df = channels_channelnames_totalvideos()
    df_sorted = df.sort_values(by='Total Videos', ascending=True)
    col5, col6 = st.columns(2)
    with col5:
        st.dataframe(df)
    with col6:
        fig = px.bar(df_sorted, x='Total Videos', y='Channel Names', template='seaborn')
        fig.update_traces(text=df_sorted['Total Videos'], textposition='outside')
        colors = px.colors.qualitative.Plotly
        fig.update_traces(marker=dict(color=colors[:len(df_sorted)]))
        st.plotly_chart(fig, use_container_width=True)

    # Date wise Published Videos
    st.subheader('Date wise Published Videos')
    current_date = datetime.datetime.now().date()
    current_year = datetime.datetime.now().year
    year_startdate = datetime.date(current_year, 1, 1)
    start_date = st.date_input('Start Date', value=year_startdate)
    end_date = st.date_input('End Date', value=current_date, max_value=current_date)
    df = channels_channelnames_publishvideos(start_date, end_date)
    df_sorted = df.sort_values(by='Published videos', ascending=True)
    col19, col20 = st.columns(2)
    with col19:
        st.dataframe(df)
    with col20:
        fig = px.pie(df_sorted, names='Channel Names', values='Published videos', hole=0.5)
        fig.update_traces(text=df_sorted['Channel Names'], textinfo='percent+value', textposition='outside',
                          textfont=dict(color='white'))
        st.plotly_chart(fig, use_container_width=True)

    # Channel wise Subscrptions pie
    df = channels_channelnames_subscriptions()
    df_sorted = df.sort_values(by='Total Subscriptions', ascending=True)
    col9, col10 = st.columns(2)
    with col9:
        st.subheader('Channel wise Subscriptions')
        st.dataframe(df)
    with col10:
        fig = px.pie(df_sorted, names='Channel Names', values='Total Subscriptions', hole=0)
        fig.update_traces(text=df_sorted['Channel Names'], textinfo='percent+value', textposition='auto',
                          insidetextfont=dict(color='white'))
        st.plotly_chart(fig, use_container_width=True)

    # Channel wise Views bar
    df = channels_channelnames_views()
    df_sorted = df.sort_values(by='Total Views', ascending=True)
    col11, col12 = st.columns(2)
    with col11:
        st.dataframe(df)
    with col12:
        fig = px.bar(df_sorted, x='Total Views', y='Channel Names', template='seaborn')
        fig.update_traces(text=df_sorted['Total Views'], textposition='auto')
        colors = px.colors.qualitative.Plotly
        fig.update_traces(marker=dict(color=colors[:len(df_sorted)]))
        st.plotly_chart(fig, use_container_width=True)

    # Channel wise  Likes pie
    df = channels_channelnames_totallikes()
    df_sorted = df.sort_values(by='Total Likes', ascending=True)
    col13, col14 = st.columns(2)
    with col13:
        st.subheader('Channel wise  Likes')
        st.dataframe(df)
    with col14:
        fig = px.pie(df_sorted, names='Channel Names', values='Total Likes', hole=0)
        fig.update_traces(text=df_sorted['Channel Names'], textinfo='percent+value', textposition='auto',
                          textfont=dict(color='white'))
        st.plotly_chart(fig, use_container_width=True)

    # Channel wise Comments bar
    df = channels_channelnames_totalcomments()
    df_sorted = df.sort_values(by='Total Comments', ascending=True)
    col15, col16 = st.columns(2)
    with col15:
        st.dataframe(df)
    with col16:
        fig = px.bar(df_sorted, x='Total Comments', y='Channel Names', template='seaborn')
        fig.update_traces(text=df_sorted['Total Comments'], textposition='auto')
        colors = px.colors.qualitative.Plotly
        fig.update_traces(marker=dict(color=colors[:len(df_sorted)]), insidetextfont=dict(color='black'))
        st.plotly_chart(fig, use_container_width=True)

    # Channel wise Video Total Durations pie
    df = channels_channelnames_totaldurations()
    df_sorted = df.sort_values(by='Total Durations', ascending=False)
    col17, col18 = st.columns(2)
    with col17:
        st.subheader('Channel wise Total Durations')
        st.dataframe(df)
    with col18:
        df_sorted['Total Durations'] = pd.to_timedelta(df_sorted['Total Durations']).dt.total_seconds()
        fig = px.pie(df_sorted, names='Channel Names', values='Total Durations', hole=0.5)
        fig.update_traces(text=df_sorted['Channel Names'], textinfo='percent', textposition='outside')
        st.plotly_chart(fig, use_container_width=True)

    # Channel wise Video Average Durations pie
    df = channels_channelnames_avgdurations()
    df_sorted = df.sort_values(by='Average Durations', ascending=False)
    col19, col20 = st.columns(2)
    with col19:
        st.subheader('Channel wise Average Durations')
        st.dataframe(df)
    with col20:
        df_sorted['Average Durations'] = pd.to_timedelta(df_sorted['Average Durations']).dt.total_seconds()
        fig = px.pie(df_sorted, names='Channel Names', values='Average Durations', hole=0.5)
        fig.update_traces(text=df_sorted['Channel Names'], textinfo='percent', textposition='outside',
                          textfont=dict(color='white'))
        st.plotly_chart(fig, use_container_width=True)


# ---------------------- video based data analysis -----------------------------------

# all channels - list of all videos of all videos
def videos_videonames_channelnames():
    st.subheader('Videos wise Channels')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select video.video_name, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_name\
                    order by channel.channel_name, video.video_name ASC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Video Names', 'Channel Names'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# selected channel - list of all videos of selected channel
def videos_videonames_selectchannel(channel):
    st.subheader('Videos wise  Channels')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute('select video.video_name, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    where channel.channel_name = \'' + channel + '\'\
                    group by video.video_id, channel.channel_name\
                    order by video.video_name ASC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Video Names', 'Channel Names'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# all channels - videos wise total views
def videos_videonames_totalviews():
    st.subheader('Videos wise Views')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select video.video_name, video.view_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_name\
                    order by video.view_count DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Video Names', 'Total Views', 'Channel Names'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# selected channels - videos wise total views
def videos_videonames_selectviews(channel):
    st.subheader('Videos wise Views')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute('select video.video_name, video.view_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    where channel.channel_name = \'' + channel + '\'\
                    group by video.video_id, channel.channel_name\
                    order by video.view_count DESC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Video Names', 'Total Views', 'Channel Names'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# all channels - videos wise total likes
def videos_videonames_totallikes():
    st.subheader('Videos wise Likes')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select video.video_name, video.like_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_name\
                    order by video.like_count DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Video Names', 'Total Likes', 'Channel Names'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# selected channels - videos wise total likes
def videos_videonames_selectlikes(channel):
    st.subheader('Videos wise Likes')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute('select video.video_name, video.like_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    where channel.channel_name = \'' + channel + '\'\
                    group by video.video_id, channel.channel_name\
                    order by video.like_count DESC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Video Names', 'Total Likes', 'Channel Names'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# all channels - videos wise total comments count
def videos_videonames_totalcommentscount():
    st.subheader('Videos wise Total Comments')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select video.video_name, video.comment_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_name\
                    order by video.comment_count DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Video Names', 'Total Comments', 'Channel Names'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# selected channels - videos wise total comments count
def videos_videonames_selectcommentscount(channel):
    st.subheader('Videos wise Total Comments')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute('select video.video_name, video.comment_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    where channel.channel_name = \'' + channel + '\'\
                    group by video.video_id, channel.channel_name\
                    order by video.comment_count DESC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Video Names', 'Total Comments', 'Channel Names'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# all channels - videos wise total comments
def videos_videonames_totalcomments():
    st.subheader('Videos wise Comments')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select video.video_name, comment.comment_text, channel.channel_name\
                    from comment\
                    inner join video on video.video_id = comment.video_id\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_id, comment.comment_id\
                    order by channel.channel_name, video.video_name, comment.comment_text ASC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Video Names', 'Comment Names', 'Channel Names'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# selected channels - videos wise total comments
def videos_videonames_selectcomments(channel):
    st.subheader('Videos wise Comments')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute('select video.video_name, comment.comment_text, channel.channel_name\
                    from comment\
                    inner join video on video.video_id = comment.video_id\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    where channel.channel_name = \'' + channel + '\'\
                    group by video.video_id, channel.channel_id, comment.comment_id\
                    order by channel.channel_name, video.video_name, comment.comment_text ASC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Video Names', 'Comment Names', 'Channel Names'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# all channels - videos wise total video durations
def videos_videonames_totaldurations():
    st.subheader('Videos wise Durations')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select video.video_name, video.duration, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_name\
                    order by video.duration DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Video Names', 'Total Durations', 'Channel Names'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# selected channels - videos wise total video durations
def videos_videonames_selectdurations(channel):
    st.subheader('Videos wise Durations')
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute('select video.video_name, video.duration, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    where channel.channel_name = \'' + channel + '\'\
                    group by video.video_id, channel.channel_name\
                    order by video.duration DESC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    df = pd.DataFrame(s, columns=['Video Names', 'Total Durations', 'Channel Names'], index=i)
    df = df.rename_axis('S.No')
    df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
    return df


# combine all channels and videos def functions in single function
def analysis_videos():
    option_videos = st.selectbox('', list_channel)
    if option_videos == 'Over All':
        st.dataframe(videos_videonames_channelnames())
        st.dataframe(videos_videonames_totalviews())
        st.dataframe(videos_videonames_totallikes())
        st.dataframe(videos_videonames_totalcommentscount())
        st.dataframe(videos_videonames_totalcomments())
        st.dataframe(videos_videonames_totaldurations())
    else:
        st.dataframe(videos_videonames_selectchannel(option_videos))
        st.dataframe(videos_videonames_selectviews(option_videos))
        st.dataframe(videos_videonames_selectlikes(option_videos))
        st.dataframe(videos_videonames_selectcommentscount(option_videos))
        st.dataframe(videos_videonames_selectcomments(option_videos))
        st.dataframe(videos_videonames_selectdurations(option_videos))


# ---------------------- sample SQL queries ------------------------------------

def q1_allvideoname_channelname():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute('select video.video_name, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_id\
                    order by channel.channel_name ASC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Video Names', 'Channel Names'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data


def q2_channelname_totalvideos():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select distinct channel.channel_name, count(distinct video.video_id) as total\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by channel.channel_id\
                    order by total DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Channel Names', 'Total Videos'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data


def q3_mostviewvideos_channelname():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select distinct video.video_name, video.view_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    order by video.view_count DESC\
                    limit 10")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Video Names', 'Total Views', 'Channel Names'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data


def q4_videonames_totalcomments():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute('select video.video_name, video.comment_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_name\
                    order by video.comment_count DESC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Video Names', 'Total Comments', 'Channel Names'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data


def q5_videonames_highestlikes_channelname():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select distinct video.video_name, channel.channel_name, video.like_count\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    where video.like_count = (select max(like_count) from video)")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Video Names', 'Channel Names', 'Most Likes'], index=i)
    data = data.reindex(columns=['Video Names', 'Most Likes', 'Channel Names'])
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data


def q6_videonames_totallikes_channelname():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select distinct video.video_name, video.like_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_id\
                    order by video.like_count DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Video Names', 'Total Likes', 'Channel Names'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data


def q7_channelnames_totalviews():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select channel_name, channel_views from channel\
                    order by channel_views DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    data = pd.DataFrame(s, columns=['Channel Names', 'Total Views'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data


def q8_channelnames_releasevideos(year):
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute('select distinct channel.channel_name, count(distinct video.video_id) as total\
        from video\
        inner join playlist on playlist.upload_id = video.upload_id\
        inner join channel on channel.channel_id = playlist.channel_id\
        where extract(year from video.published_date) = \'' + str(year) + '\'\
        group by channel.channel_id\
        order by total DESC')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    data = pd.DataFrame(s, columns=['Channel Names', 'Published Videos'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data


def q9_channelnames_avgvideoduration():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute("select channel.channel_name, substring(cast(avg(video.duration) as varchar), 1, 8) as average\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by channel.channel_id\
                    order by average DESC")
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    data = pd.DataFrame(s, columns=['Channel Names', 'Average Video Duration'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data


def q10_videonames_channelnames_mostcomments():
    gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
    cursor = gopi_s.cursor()
    cursor.execute('select video.video_name, video.comment_count, channel.channel_name\
                    from video\
                    inner join playlist on playlist.upload_id = video.upload_id\
                    inner join channel on channel.channel_id = playlist.channel_id\
                    group by video.video_id, channel.channel_name\
                    order by video.comment_count DESC\
                    limit 1')
    s = cursor.fetchall()
    i = [i for i in range(1, len(s) + 1)]
    pd.set_option('display.max_columns', None)
    data = pd.DataFrame(s, columns=['Video Names', 'Channel Names', 'Total Comments'], index=i)
    data = data.rename_axis('S.No')
    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
    return data


# combine all quesries in single function
def sql_queries():
    st.subheader('Select the Query below')
    q1 = 'Q1-What are the names of all the videos and their corresponding channels?'
    q2 = 'Q2-Which channels have the most number of videos, and how many videos do they have?'
    q3 = 'Q3-What are the top 10 most viewed videos and their respective channels?'
    q4 = 'Q4-How many comments were made on each video with their corresponding video names?'
    q5 = 'Q5-Which videos have the highest number of likes with their corresponding channel names?'
    q6 = 'Q6-What is the total number of likes for each video with their corresponding video names?'
    q7 = 'Q7-What is the total number of views for each channel with their corresponding channel names?'
    q8 = 'Q8-What are the names of all the channels that have published videos in the particular year?'
    q9 = 'Q9-What is the average duration of all videos in each channel with corresponding channel names?'
    q10 = 'Q10-Which videos have the highest number of comments with their corresponding channel names?'

    query_option = st.selectbox('', ['Select One', q1, q2, q3, q4, q5, q6, q7, q8, q9, q10])
    if query_option == q1:
        st.dataframe(q1_allvideoname_channelname())
    elif query_option == q2:
        st.dataframe(q2_channelname_totalvideos())
    elif query_option == q3:
        st.dataframe(q3_mostviewvideos_channelname())
    elif query_option == q4:
        st.dataframe(q4_videonames_totalcomments())
    elif query_option == q5:
        st.dataframe(q5_videonames_highestlikes_channelname())
    elif query_option == q6:
        st.dataframe(q6_videonames_totallikes_channelname())
    elif query_option == q7:
        st.dataframe(q7_channelnames_totalviews())
    elif query_option == q8:
        year = st.text_input('Enter the year')
        submit = st.button('Submit')
        if submit:
            st.dataframe(q8_channelnames_releasevideos(year))
    elif query_option == q9:
        st.dataframe(q9_channelnames_avgvideoduration())
    elif query_option == q10:
        st.dataframe(q10_videonames_channelnames_mostcomments())


st.title('YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit')
st.header('Please select the option below:')
# MongoDB database name
database = 'project_youtube'
st.code('1 - Retrieving data from the YouTube API')
st.code('2 - Store data to MongoDB')
st.code('3 - Migrating data to a SQL data warehouse')
st.code('4 - Data Analysis')
st.code('5 - SQL Queries')
st.code('6 - Exit')
list_options = ['Select one', 'Retrieving data from the YouTube API', 'Store data to MongoDB',
                'Migrating data to a SQL data warehouse', 'Data Analysis', 'SQL Queries', 'Exit']
option = st.selectbox('', list_options)

if option:
    try:
        if option == 'Retrieving data from the YouTube API':
            channel_id = st.text_input("Enter Channel ID: ")  # get a input from user
            api_key = st.text_input("Enter Your API Key:", type='password')  # get a API key from user
            api_service_name = "youtube"
            api_version = "v3"
            youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
            submit_c = st.button('Enter')
            if submit_c:
                data_youtube = {}
                data_extraction = data_extraction_youtube(channel_id)
                data_youtube.update(data_extraction)
                channel_name = data_youtube['channel_name']['channel_name']

                temp_collection_drop()
                data_store_mongodb(channel_name=channel_name, database='temp', data_youtube=data_youtube)
                # display the sample data in streamlit
                st.json(display_sample_data(channel_id))
                st.success('Retrived data from YouTube successfully')
                st.balloons()

        elif option == 'Store data to MongoDB':
            mongodb(database)

        elif option == 'Migrating data to a SQL data warehouse':
            sql(database)

        elif option == 'Data Analysis':
            gopi_s = psycopg2.connect(host='localhost', user='postgres', password='root', database='youtube')
            cursor = gopi_s.cursor()
            cursor.execute('select channel_name from channel order by channel_name ASC')
            s = cursor.fetchall()
            list_channel = ['Over All']
            list_channel.extend([i[0] for i in s])
            st.subheader('Please select the option below:')
            analysis = ['Select one', 'Channels', 'Videos']
            select_analysis = st.selectbox('', analysis)
            if select_analysis == 'Channels':
                analysis_channels()
            elif select_analysis == 'Videos':
                analysis_videos()

        elif option == 'SQL Queries':
            sql_queries()

        elif option == 'Exit':
            temp_collection_drop()
            st.success('Thank you for your time. Exiting the application')
            st.balloons()

        else:
            st.warning("Please select the option")
    except:
        st.error("Please enter the valid Channel ID and API key")
