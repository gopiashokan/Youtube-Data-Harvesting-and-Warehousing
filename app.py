import datetime
import googleapiclient.discovery
import pandas as pd
import psycopg2
import pymongo
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


def streamlit_config():

    # page configuration
    st.set_page_config(page_title='YouTube Data Harvesting and Warehousing',
                       page_icon=':bar_chart:', layout="wide")

    # page header transparent color
    page_background_color = """
    <style>

    [data-testid="stHeader"] 
    {
    background: rgba(0,0,0,0);
    }

    </style>
    """
    st.markdown(page_background_color, unsafe_allow_html=True)

    # title and position
    st.markdown(f'<h1 style="text-align: center;">YouTube Data Harvesting and Warehousing</h1>',
                unsafe_allow_html=True)


class youtube_extract:

    def channel(youtube, channel_id):

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
                'country': response['items'][0]['snippet'].get('country', 'Not Available')}
        return data

    def playlist(youtube, channel_id, upload_id):

        request = youtube.playlists().list(
            part="snippet,contentDetails,status",
            channelId=channel_id,
            maxResults=50)
        response = request.execute()

        playlist = []

        for i in range(0, len(response['items'])):
            data = {'playlist_id': response['items'][i]['id'],
                    'playlist_name': response['items'][i]['snippet']['title'],
                    'channel_id': channel_id,
                    'upload_id': upload_id}

            playlist.append(data)

        next_page_token = response.get('nextPageToken')

        # manually set umbrella = True for breaking while condition
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

                    playlist.append(data)

                next_page_token = response.get('nextPageToken')

        return playlist

    def video_ids(youtube, upload_id):

        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=upload_id,
            maxResults=50)
        response = request.execute()

        video_ids = []

        for i in range(0, len(response['items'])):
            data = response['items'][i]['contentDetails']['videoId']
            video_ids.append(data)

        next_page_token = response.get('nextPageToken')

        # manually set umbrella = True for breaking while condition
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
                    video_ids.append(data)

                next_page_token = response.get('nextPageToken')

        return video_ids

    def video(youtube, video_id, upload_id):

        request = youtube.videos().list(
            part='contentDetails, snippet, statistics',
            id=video_id)
        response = request.execute()

        caption = {'true': 'Available', 'false': 'Not Available'}

        # convert PT15M33S to 00:15:33 format using Timedelta function in pandas

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
                'caption_status': caption[response['items'][0]['contentDetails']['caption']]}

        if data['tags'] == []:
            del data['tags']

        return data

    def comment(youtube, video_id):

        request = youtube.commentThreads().list(
            part='id, snippet',
            videoId=video_id,
            maxResults=100)
        response = request.execute()

        for i in range(0, len(response['items'])):
            data = {'comment_id': response['items'][i]['id'],
                    'comment_text': response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'comment_author': response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'comment_published_date': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][0:10],
                    'comment_published_time': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][11:19],
                    'video_id': video_id}

        return data

    def main(channel_id):

        channel = youtube_extract.channel(youtube, channel_id)
        upload_id = channel['upload_id']
        playlist = youtube_extract.playlist(youtube, channel_id, upload_id)
        video_ids = youtube_extract.video_ids(youtube, upload_id)

        video = []
        comment = []

        for i in video_ids:
            v = youtube_extract.video(youtube, i, upload_id)
            video.append(v)

            # skip disabled comments error in looping function
            try:
                c = youtube_extract.comment(youtube, i)
                comment.append(c)
            except:
                pass

        final = {'channel': channel,
                 'playlist': playlist,
                 'video': video,
                 'comment': comment}

        return final

    def display_sample_data(channel_id):

        channel = youtube_extract.channel(youtube, channel_id)
        upload_id = channel['upload_id']
        playlist = youtube_extract.playlist(youtube, channel_id, upload_id)
        video_ids = youtube_extract.video_ids(youtube, upload_id)

        video = []
        comment = []

        for i in video_ids:
            v = youtube_extract.video(youtube, i, upload_id)
            video.append(v)

            # skip disabled comments error in looping function
            try:
                c = youtube_extract.comment(youtube, i)
                comment.append(c)
            except:
                pass
            break

        final = {'channel': channel,
                 'playlist': playlist,
                 'video': video,
                 'comment': comment}

        return final


class mongodb:

    def list_collection_names(database):
        gopi = pymongo.MongoClient(
            "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
        db = gopi[database]
        col = db.list_collection_names()
        col.sort(reverse=False)
        return col

    def order_collection_names(database):

        m = mongodb.list_collection_names(database)

        if m == []:
            st.info("The Mongodb database is currently empty")

        else:
            st.subheader('List of collections in MongoDB database')
            m = mongodb.list_collection_names(database)
            c = 1
            for i in m:
                st.write(str(c) + ' - ' + i)
                c += 1

    def data_storage(channel_name, database, data):
        gopi = pymongo.MongoClient(
            "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
        db = gopi[database]
        col = db[channel_name]
        col.insert_one(data)

    def drop_temp_collection():
        gopi = pymongo.MongoClient(
            "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
        db = gopi['temp']
        col = db.list_collection_names()
        if len(col) > 0:
            for i in col:
                db.drop_collection(i)

    def main(database):

        gopi = pymongo.MongoClient(
            "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
        db = gopi['temp']
        col = db.list_collection_names()

        if len(col) == 0:
            st.info("There is no data retrived from youtube")

        else:
            gopi = pymongo.MongoClient(
                "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
            db = gopi['temp']
            col = db.list_collection_names()
            channel_name = col[0]

            # Now we get the channel name and access channel data
            data_youtube = {}
            col1 = db[channel_name]
            for i in col1.find():
                data_youtube.update(i)

            # verify channel name already exists in database
            list_collection_names = mongodb.list_collection_names(database)

            if channel_name not in list_collection_names:
                mongodb.data_storage(channel_name, database, data_youtube)
                st.success(
                    "The data has been successfully stored in the MongoDB database")
                st.balloons()
                mongodb.drop_temp_collection()

            else:
                st.warning(
                    "The data has already been stored in MongoDB database")
                option = st.radio('Do you want to overwrite the data currently stored?',
                                  ['Select one below', 'Yes', 'No'])

                if option == 'Yes':
                    gopi = pymongo.MongoClient(
                        "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
                    db = gopi[database]

                    # delete existing data
                    db[channel_name].drop()

                    # add new data
                    mongodb.data_storage(channel_name, database, data_youtube)
                    st.success(
                        "The data has been successfully overwritten and updated in MongoDB database")
                    st.balloons()
                    mongodb.drop_temp_collection()

                elif option == 'No':
                    mongodb.drop_temp_collection()
                    st.info("The data overwrite process has been skipped")


class sql:

    def create_tables():

        gopi = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='root',
                                database='youtube')
        cursor = gopi.cursor()

        cursor.execute(f"""create table if not exists channel(
                                    channel_id 			varchar(255) primary key,
                                    channel_name		varchar(255),
                                    subscription_count	int,
                                    channel_views		int,
                                    channel_description	text,
                                    upload_id			varchar(255),
                                    country				varchar(255));""")

        cursor.execute(f"""create table if not exists playlist(
                                    playlist_id		varchar(255) primary key,
                                    playlist_name	varchar(255),
                                    channel_id		varchar(255),
                                    upload_id		varchar(255));""")

        cursor.execute(f"""create table if not exists video(
                                    video_id			varchar(255) primary key,
                                    video_name			varchar(255),
                                    video_description	text,
                                    upload_id			varchar(255),
                                    tags				text,
                                    published_date		date,
                                    published_time		time,
                                    view_count			int,
                                    like_count			int,
                                    favourite_count		int,
                                    comment_count		int,
                                    duration			time,
                                    thumbnail			varchar(255),
                                    caption_status		varchar(255));""")

        cursor.execute(f"""create table if not exists comment(
                                    comment_id				varchar(255) primary key,
                                    comment_text			text,
                                    comment_author			varchar(255),
                                    comment_published_date	date,
                                    comment_published_time	time,
                                    video_id				varchar(255));""")

        gopi.commit()

    def list_channel_names():

        gopi = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='root',
                                database='youtube')
        cursor = gopi.cursor()
        cursor.execute("select channel_name from channel")
        s = cursor.fetchall()
        s = [i[0] for i in s]
        s.sort(reverse=False)
        return s

    def order_channel_names():

        s = sql.list_channel_names()

        if s == []:
            st.info("The SQL database is currently empty")

        else:
            st.subheader("List of channels in SQL database")
            c = 1
            for i in s:
                st.write(str(c) + ' - ' + i)
                c += 1

    def channel(database, channel_name):

        gopi = pymongo.MongoClient(
            "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
        db = gopi[database]
        col = db[channel_name]

        data = []
        for i in col.find({}, {'_id': 0, 'channel': 1}):
            data.append(i['channel'])

        df = pd.DataFrame(data)
        df = df.reindex(columns=['channel_id', 'channel_name', 'subscription_count', 'channel_views',
                                 'channel_description', 'upload_id', 'country'])
        df['subscription_count'] = pd.to_numeric(df['subscription_count'])
        df['channel_views'] = pd.to_numeric(df['channel_views'])
        return df

    def playlist(database, channel_name):

        gopi = pymongo.MongoClient(
            "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
        db = gopi[database]
        col = db[channel_name]

        data = []
        for i in col.find({}, {'_id': 0, 'playlist': 1}):
            data.extend(i['playlist'])

        df = pd.DataFrame(data)
        df = df.reindex(
            columns=['playlist_id', 'playlist_name', 'channel_id', 'upload_id'])
        return df

    def video(database, channel_name):

        gopi = pymongo.MongoClient(
            "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
        db = gopi[database]
        col = db[channel_name]

        data = []
        for i in col.find({}, {'_id': 0, 'video': 1}):
            data.extend(i['video'])

        df = pd.DataFrame(data)
        df = df.reindex(columns=['video_id', 'video_name', 'video_description', 'upload_id',
                                 'tags', 'published_date', 'published_time', 'view_count',
                                 'like_count', 'favourite_count', 'comment_count', 'duration',
                                 'thumbnail', 'caption_status'])

        df['published_date'] = pd.to_datetime(df['published_date']).dt.date
        df['published_time'] = pd.to_datetime(
            df['published_time'], format='%H:%M:%S').dt.time
        df['view_count'] = pd.to_numeric(df['view_count'])
        df['like_count'] = pd.to_numeric(df['like_count'])
        df['favourite_count'] = pd.to_numeric(df['favourite_count'])
        df['comment_count'] = pd.to_numeric(df['comment_count'])
        df['duration'] = pd.to_datetime(
            df['duration'], format='%H:%M:%S').dt.time
        return df

    def comment(database, channel_name):
        gopi = pymongo.MongoClient(
            "mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
        db = gopi[database]
        col = db[channel_name]

        data = []
        for i in col.find({}, {'_id': 0, 'comment': 1}):
            data.extend(i['comment'])

        df = pd.DataFrame(data)
        df = df.reindex(columns=['comment_id', 'comment_text', 'comment_author',
                                 'comment_published_date', 'comment_published_time', 'video_id'])
        df['comment_published_date'] = pd.to_datetime(
            df['comment_published_date']).dt.date
        df['comment_published_time'] = pd.to_datetime(
            df['comment_published_time'], format='%H:%M:%S').dt.time
        return df

    def main(mdb_database, sql_database):

        # create table in sql
        sql.create_tables()

        # mongodb and sql channel names
        m = mongodb.list_collection_names(mdb_database)
        s = sql.list_channel_names()

        if s == m == []:
            st.info("Both Mongodb and SQL databases are currently empty")

        else:
            # mongodb and sql channel names
            mongodb.order_collection_names(mdb_database)
            sql.order_channel_names()

            # remaining channel name for migration
            list_mongodb_notin_sql = ['Select one']
            m = mongodb.list_collection_names(mdb_database)
            s = sql.list_channel_names()

            # verify channel name not in sql
            for i in m:
                if i not in s:
                    list_mongodb_notin_sql.append(i)

            # channel name for user selection
            option = st.selectbox('', list_mongodb_notin_sql)

            if option == 'Select one':
                col1, col2 = st.columns(2)
                with col1:
                    st.warning('Please select the channel')

            else:
                channel = sql.channel(sql_database, option)
                playlist = sql.playlist(sql_database, option)
                video = sql.video(sql_database, option)
                comment = sql.comment(sql_database, option)

                gopi = psycopg2.connect(host='localhost',
                                        user='postgres',
                                        password='root',
                                        database='youtube')
                cursor = gopi.cursor()

                cursor.executemany(f"""insert into channel(channel_id, channel_name, subscription_count,
                                        channel_views, channel_description, upload_id, country) 
                                        values(%s,%s,%s,%s,%s,%s,%s)""", channel.values.tolist())

                cursor.executemany(f"""insert into playlist(playlist_id, playlist_name, channel_id, 
                                        upload_id) 
                                        values(%s,%s,%s,%s)""", playlist.values.tolist())

                cursor.executemany(f"""insert into video(video_id, video_name, video_description, 
                                        upload_id, tags, published_date, published_time, view_count, 
                                        like_count, favourite_count, comment_count, duration, thumbnail, 
                                        caption_status) 
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                   video.values.tolist())

                cursor.executemany(f"""insert into comment(comment_id, comment_text, comment_author, 
                                        comment_published_date, comment_published_time, video_id) 
                                        values(%s,%s,%s,%s,%s,%s)""", comment.values.tolist())

                gopi.commit()
                st.success("Migrated Data Successfully to SQL Data Warehouse")
                st.balloons()
                gopi.close()


class sql_queries:

    def q1_allvideoname_channelname():
        gopi_s = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='youtube')
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
        data = pd.DataFrame(
            s, columns=['Video Names', 'Channel Names'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def q2_channelname_totalvideos():
        gopi_s = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='youtube')
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
        data = pd.DataFrame(
            s, columns=['Channel Names', 'Total Videos'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def q3_mostviewvideos_channelname():
        gopi_s = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='youtube')
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
        data = pd.DataFrame(
            s, columns=['Video Names', 'Total Views', 'Channel Names'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def q4_videonames_totalcomments():
        gopi_s = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='youtube')
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
        data = pd.DataFrame(
            s, columns=['Video Names', 'Total Comments', 'Channel Names'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def q5_videonames_highestlikes_channelname():
        gopi_s = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='youtube')
        cursor = gopi_s.cursor()
        cursor.execute("select distinct video.video_name, channel.channel_name, video.like_count\
                        from video\
                        inner join playlist on playlist.upload_id = video.upload_id\
                        inner join channel on channel.channel_id = playlist.channel_id\
                        where video.like_count = (select max(like_count) from video)")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        pd.set_option('display.max_columns', None)
        data = pd.DataFrame(
            s, columns=['Video Names', 'Channel Names', 'Most Likes'], index=i)
        data = data.reindex(
            columns=['Video Names', 'Most Likes', 'Channel Names'])
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def q6_videonames_totallikes_channelname():
        gopi_s = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='youtube')
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
        data = pd.DataFrame(
            s, columns=['Video Names', 'Total Likes', 'Channel Names'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def q7_channelnames_totalviews():
        gopi_s = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='youtube')
        cursor = gopi_s.cursor()
        cursor.execute("select channel_name, channel_views from channel\
                        order by channel_views DESC")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(
            s, columns=['Channel Names', 'Total Views'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def q8_channelnames_releasevideos(year):
        gopi_s = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='youtube')
        cursor = gopi_s.cursor()
        cursor.execute(f"""select distinct channel.channel_name, count(distinct video.video_id) as total
            from video
            inner join playlist on playlist.upload_id = video.upload_id
            inner join channel on channel.channel_id = playlist.channel_id
            where extract(year from video.published_date) = '{year}'
            group by channel.channel_id
            order by total DESC""")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(
            s, columns=['Channel Names', 'Published Videos'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def q9_channelnames_avgvideoduration():
        gopi_s = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='youtube')
        cursor = gopi_s.cursor()
        cursor.execute("select channel.channel_name, substring(cast(avg(video.duration) as varchar), 1, 8) as average\
                        from video\
                        inner join playlist on playlist.upload_id = video.upload_id\
                        inner join channel on channel.channel_id = playlist.channel_id\
                        group by channel.channel_id\
                        order by average DESC")
        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        data = pd.DataFrame(
            s, columns=['Channel Names', 'Average Video Duration'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def q10_videonames_channelnames_mostcomments():
        gopi_s = psycopg2.connect(
            host='localhost', user='postgres', password='root', database='youtube')
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
        data = pd.DataFrame(
            s, columns=['Video Names', 'Channel Names', 'Total Comments'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def main():
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

        query_option = st.selectbox(
            '', ['Select One', q1, q2, q3, q4, q5, q6, q7, q8, q9, q10])

        if query_option == q1:
            st.dataframe(sql_queries.q1_allvideoname_channelname())

        elif query_option == q2:
            st.dataframe(sql_queries.q2_channelname_totalvideos())

        elif query_option == q3:
            st.dataframe(sql_queries.q3_mostviewvideos_channelname())

        elif query_option == q4:
            st.dataframe(sql_queries.q4_videonames_totalcomments())

        elif query_option == q5:
            st.dataframe(sql_queries.q5_videonames_highestlikes_channelname())

        elif query_option == q6:
            st.dataframe(sql_queries.q6_videonames_totallikes_channelname())

        elif query_option == q7:
            st.dataframe(sql_queries.q7_channelnames_totalviews())

        elif query_option == q8:
            year = st.text_input('Enter the year')
            submit = st.button('Submit')
            if submit:
                st.dataframe(sql_queries.q8_channelnames_releasevideos(year))

        elif query_option == q9:
            st.dataframe(sql_queries.q9_channelnames_avgvideoduration())

        elif query_option == q10:
            st.dataframe(
                sql_queries.q10_videonames_channelnames_mostcomments())


class channel_analysis:

    def total_channel_names():

        st.subheader('List of Channels')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(
            "select channel_name from channel order by channel_name ASC")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(s, columns=['Channel Names'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_playlist_names():

        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select distinct playlist.playlist_name, channel.channel_name
                        from playlist
                        inner join channel on playlist.channel_id = channel.channel_id
                        group by playlist.playlist_name, channel.channel_name
                        order by channel.channel_name, playlist.playlist_name ASC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Playlist Names', 'Channel Names'], index=i)
        df = df.reindex(columns=['Channel Names', 'Playlist Names'])
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_playlist_names_select_channel(channel_name):

        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select distinct playlist.playlist_name, channel.channel_name
                            from playlist
                            inner join channel on playlist.channel_id = channel.channel_id
                            where channel.channel_name='{channel_name}'
                            group by playlist.playlist_id, channel.channel_name
                            order by channel.channel_name, playlist.playlist_name ASC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Playlist Names', 'Channel Names'], index=i)
        df = df.reindex(columns=['Channel Names', 'Playlist Names'])
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_playlist_count():

        st.subheader('Channel wise Playlists')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select distinct channel.channel_name, count(distinct playlist.playlist_id) as total
                        from playlist
                        inner join channel on channel.channel_id = playlist.channel_id
                        group by channel.channel_id
                        order by total DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Channel Names', 'Total Playlists'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_video_count():

        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select channel.channel_name, count(distinct video.video_id) as total
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        group by channel.channel_id
                        order by total DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Channel Names', 'Total Videos'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def published_videos_count(start_date, end_date):

        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select distinct channel.channel_name, count(distinct video.video_id) as total
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        where video.published_date between '{start_date}' and '{end_date}'
                        group by channel.channel_id
                        order by total DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Channel Names', 'Published videos'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_subscriptions():

        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select channel_name, subscription_count 
                           from channel
                           order by subscription_count DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Channel Names', 'Total Subscriptions'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_views():

        st.subheader('Channel wise  Views')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select channel_name, channel_views 
                           from channel
                           order by channel_views DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(s, columns=['Channel Names', 'Total Views'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_likes():

        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select distinct channel.channel_name, cast(sum(subquery.sum1) as int) as sum2
                        from channel
                        inner join(select distinct playlist.channel_id, video.video_id, sum(distinct video.like_count) as sum1
                        from playlist
                        inner join video on playlist.upload_id = video.upload_id
                        group by playlist.channel_id, video.video_id
                        )as subquery on subquery.channel_id = channel.channel_id
                        group by channel.channel_name
                        order by sum2 DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(s, columns=['Channel Names', 'Total Likes'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_comments():

        st.subheader('Channel wise  Comments')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select distinct channel.channel_name, count(distinct comment.comment_id) as total
                        from comment
                        inner join video on video.video_id = comment.video_id
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        group by channel.channel_name
                        order by total DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Channel Names', 'Total Comments'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    # convert days with time to HH:MM:SS

    def convert_durations(durations):

        days, time = durations.split(' days ')
        hours, minutes, seconds = map(int, time.split(':'))
        total_hours = int(days) * 24 + hours
        formatted_duration = f"{total_hours:02d}:{minutes:02d}:{seconds:02d}"
        return formatted_duration

    def total_durations():

        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select channel.channel_name, sum(video.duration) as total
                        from video
                        inner join channel on channel.upload_id = video.upload_id
                        group by channel.channel_id
                        order by total DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Channel Names', 'Total Durations'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        df['Total Durations'] = df['Total Durations'].apply(
            lambda x: channel_analysis.convert_durations(str(x)))
        return df

    def average_durations():

        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select channel.channel_name, 
                           SUBSTRING(CAST(avg(video.duration) AS VARCHAR), 1, 8) as average
                           from video
                           inner join channel on channel.upload_id = video.upload_id
                           group by channel.channel_id
                           order by average DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Channel Names', 'Average Durations'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def main():

        # Channel List and Channel wise Playlist Names
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(channel_analysis.total_channel_names())

        with col2:
            st.subheader('Channel wise Playlists')
            channel = st.selectbox('', list_channel)
            if channel == 'Over All':
                st.dataframe(channel_analysis.total_playlist_names())
            else:
                st.dataframe(
                    channel_analysis.total_playlist_names_select_channel(channel))

        # Channel wise Playlist Counts pie
        df = channel_analysis.total_playlist_count()
        df_sorted = df.sort_values(by='Total Playlists', ascending=True)

        col3, col4 = st.columns([1, 2])
        with col3:
            st.dataframe(df)

        with col4:
            fig = px.pie(df_sorted, names='Channel Names',
                         values='Total Playlists', hole=0.5)
            fig.update_traces(text=df_sorted['Channel Names'], textinfo='percent+label',
                              texttemplate='%{percent:.2%}', textposition='outside',
                              textfont=dict(color='white'))
            st.plotly_chart(fig, use_container_width=True)

        # Channel wise Videos bar
        st.subheader('Channel wise Videos')
        df = channel_analysis.total_video_count()
        df_sorted = df.sort_values(by='Total Videos', ascending=True)

        col5, col6 = st.columns([1, 2])
        with col5:
            st.dataframe(df)

        with col6:
            fig = px.bar(df_sorted, x='Total Videos',
                         y='Channel Names', template='seaborn')
            fig.update_traces(
                text=df_sorted['Total Videos'], textposition='outside')
            colors = px.colors.qualitative.Plotly
            fig.update_traces(marker=dict(color=colors[:len(df_sorted)]))
            st.plotly_chart(fig, use_container_width=True)

        # Date wise Published Videos
        st.subheader('Date wise Published Videos')
        current_date = datetime.datetime.now().date()
        current_year = datetime.datetime.now().year
        year_startdate = datetime.date(current_year, 1, 1)

        start_date = st.date_input('Start Date', value=year_startdate)
        end_date = st.date_input(
            'End Date', value=current_date, max_value=current_date)

        df = channel_analysis.published_videos_count(start_date, end_date)
        df_sorted = df.sort_values(by='Published videos', ascending=True)

        col19, col20 = st.columns([1, 2])
        with col19:
            st.dataframe(df)

        with col20:
            fig = px.bar(df_sorted, x='Published videos',
                         y='Channel Names', template='seaborn')
            fig.update_traces(
                text=df_sorted['Published videos'], textposition='outside')
            colors = px.colors.qualitative.Plotly
            fig.update_traces(marker=dict(color=colors[:len(df_sorted)]))
            st.plotly_chart(fig, use_container_width=True)

        # Channel wise Subscrptions pie
        df = channel_analysis.total_subscriptions()
        df_sorted = df.sort_values(by='Total Subscriptions', ascending=True)

        col9, col10 = st.columns([1, 2])
        with col9:
            st.subheader('Channel wise Subscriptions')
            st.dataframe(df)

        with col10:
            fig = px.pie(df_sorted, names='Channel Names',
                         values='Total Subscriptions', hole=0.5)
            fig.update_traces(text=df_sorted['Channel Names'], textinfo='percent+label',
                              texttemplate='%{percent:.2%}', textposition='outside',
                              insidetextfont=dict(color='white'))
            st.plotly_chart(fig, use_container_width=True)

        # Channel wise Views bar
        df = channel_analysis.total_views()
        df_sorted = df.sort_values(by='Total Views', ascending=True)

        col11, col12 = st.columns([1, 2])
        with col11:
            st.dataframe(df)

        with col12:
            fig = px.bar(df_sorted, x='Total Views',
                         y='Channel Names', template='seaborn')
            fig.update_traces(
                text=df_sorted['Total Views'], textposition='auto')
            colors = px.colors.qualitative.Plotly
            fig.update_traces(marker=dict(color=colors[:len(df_sorted)]))
            st.plotly_chart(fig, use_container_width=True)

        # Channel wise  Likes pie
        df = channel_analysis.total_likes()
        df_sorted = df.sort_values(by='Total Likes', ascending=True)

        col13, col14 = st.columns([1, 2])
        with col13:
            st.subheader('Channel wise  Likes')
            st.dataframe(df)

        with col14:
            fig = px.pie(df_sorted, names='Channel Names',
                         values='Total Likes', hole=0.5)
            fig.update_traces(text=df_sorted['Channel Names'], textinfo='percent+label',
                              texttemplate='%{percent:.2%}', textposition='outside',
                              textfont=dict(color='white'))
            st.plotly_chart(fig, use_container_width=True)

        # Channel wise Comments bar
        df = channel_analysis.total_comments()
        df_sorted = df.sort_values(by='Total Comments', ascending=True)

        col15, col16 = st.columns([1, 2])
        with col15:
            st.dataframe(df)

        with col16:
            fig = px.bar(df_sorted, x='Total Comments',
                         y='Channel Names', template='seaborn')
            fig.update_traces(
                text=df_sorted['Total Comments'], textposition='auto')
            colors = px.colors.qualitative.Plotly
            fig.update_traces(marker=dict(
                color=colors[:len(df_sorted)]),
                insidetextfont=dict(color='black'))
            st.plotly_chart(fig, use_container_width=True)

        # Channel wise Video Total Durations pie
        df = channel_analysis.total_durations()
        df_sorted = df.sort_values(by='Total Durations', ascending=False)

        col17, col18 = st.columns([1, 2])
        with col17:
            st.subheader('Channel wise Total Durations')
            st.dataframe(df)

        with col18:
            df_sorted['Total Durations'] = pd.to_timedelta(
                df_sorted['Total Durations']).dt.total_seconds()
            fig = px.pie(df_sorted, names='Channel Names',
                         values='Total Durations', hole=0.5)
            fig.update_traces(text=df_sorted['Channel Names'], textinfo='percent+label',
                              texttemplate='%{percent:.2%}', textposition='outside')
            st.plotly_chart(fig, use_container_width=True)

        # Channel wise Video Average Durations pie
        df = channel_analysis.average_durations()
        df_sorted = df.sort_values(by='Average Durations', ascending=False)

        col19, col20 = st.columns([1, 2])
        with col19:
            st.subheader('Channel wise Average Durations')
            st.dataframe(df)

        with col20:
            df_sorted['Average Durations'] = pd.to_timedelta(
                df_sorted['Average Durations']).dt.total_seconds()
            fig = px.pie(df_sorted, names='Channel Names',
                         values='Average Durations', hole=0.5)
            fig.update_traces(text=df_sorted['Channel Names'], textinfo='percent+label',
                              texttemplate='%{percent:.2%}', textposition='outside',
                              textfont=dict(color='white'))
            st.plotly_chart(fig, use_container_width=True)


class video_analysis:

    def total_video_names():

        st.subheader('Videos wise Channels')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select video.video_name, channel.channel_name
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        group by video.video_id, channel.channel_name
                        order by channel.channel_name, video.video_name ASC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(s, columns=['Video Names', 'Channel Names'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_video_names_select_channel(channel_name):

        st.subheader('Videos wise  Channels')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select video.video_name, channel.channel_name
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        where channel.channel_name = '{channel_name}'
                        group by video.video_id, channel.channel_name
                        order by video.video_name ASC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(s, columns=['Video Names', 'Channel Names'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_views():

        st.subheader('Videos wise Views')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select video.video_name, video.view_count, channel.channel_name
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        group by video.video_id, channel.channel_name
                        order by video.view_count DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Video Names', 'Total Views', 'Channel Names'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_views_select_channel(channel_name):

        st.subheader('Videos wise Views')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select video.video_name, video.view_count, channel.channel_name
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        where channel.channel_name = '{channel_name}'
                        group by video.video_id, channel.channel_name
                        order by video.view_count DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Video Names', 'Total Views', 'Channel Names'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_likes():

        st.subheader('Videos wise Likes')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select video.video_name, video.like_count, channel.channel_name
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        group by video.video_id, channel.channel_name
                        order by video.like_count DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Video Names', 'Total Likes', 'Channel Names'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_likes_select_channel(channel_name):

        st.subheader('Videos wise Likes')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select video.video_name, video.like_count, channel.channel_name
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        where channel.channel_name = '{channel_name}'
                        group by video.video_id, channel.channel_name
                        order by video.like_count DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Video Names', 'Total Likes', 'Channel Names'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_comments():

        st.subheader('Videos wise Total Comments')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select video.video_name, video.comment_count, channel.channel_name
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        group by video.video_id, channel.channel_name
                        order by video.comment_count DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Video Names', 'Total Comments', 'Channel Names'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_comments_select_channel(channel_name):

        st.subheader('Videos wise Total Comments')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select video.video_name, video.comment_count, channel.channel_name
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        where channel.channel_name = '{channel_name}'
                        group by video.video_id, channel.channel_name
                        order by video.comment_count DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Video Names', 'Total Comments', 'Channel Names'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_comments_text():

        st.subheader('Videos wise Comments')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select video.video_name, comment.comment_text, channel.channel_name
                        from comment
                        inner join video on video.video_id = comment.video_id
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        group by video.video_id, channel.channel_id, comment.comment_id
                        order by channel.channel_name, video.video_name, comment.comment_text ASC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Video Names', 'Comment Names', 'Channel Names'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_comments_text_select_channel(channel_name):

        st.subheader('Videos wise Comments')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select video.video_name, comment.comment_text, channel.channel_name
                        from comment
                        inner join video on video.video_id = comment.video_id
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        where channel.channel_name = '{channel_name}'
                        group by video.video_id, channel.channel_id, comment.comment_id
                        order by channel.channel_name, video.video_name, comment.comment_text ASC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Video Names', 'Comment Names', 'Channel Names'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_durations():

        st.subheader('Videos wise Durations')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select video.video_name, video.duration, channel.channel_name
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        group by video.video_id, channel.channel_name
                        order by video.duration DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Video Names', 'Total Durations', 'Channel Names'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def total_durations_select_channel(channel_name):

        st.subheader('Videos wise Durations')
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select video.video_name, video.duration, channel.channel_name
                        from video
                        inner join playlist on playlist.upload_id = video.upload_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        where channel.channel_name = '{channel_name}'
                        group by video.video_id, channel.channel_name
                        order by video.duration DESC""")

        s = cursor.fetchall()
        i = [i for i in range(1, len(s) + 1)]
        df = pd.DataFrame(
            s, columns=['Video Names', 'Total Durations', 'Channel Names'], index=i)
        df = df.rename_axis('S.No')
        df.index = df.index.map(lambda x: '{:^{}}'.format(x, 10))
        return df

    def main():

        channel_name = st.selectbox('', list_channel)

        if channel_name == 'Over All':
            st.dataframe(video_analysis.total_video_names())
            st.dataframe(video_analysis.total_views())
            st.dataframe(video_analysis.total_likes())
            st.dataframe(video_analysis.total_comments())
            st.dataframe(video_analysis.total_comments_text())
            st.dataframe(video_analysis.total_durations())

        else:
            st.dataframe(
                video_analysis.total_video_names_select_channel(channel_name))
            st.dataframe(
                video_analysis.total_views_select_channel(channel_name))
            st.dataframe(
                video_analysis.total_likes_select_channel(channel_name))
            st.dataframe(
                video_analysis.total_comments_select_channel(channel_name))
            st.dataframe(
                video_analysis.total_comments_text_select_channel(channel_name))
            st.dataframe(
                video_analysis.total_durations_select_channel(channel_name))


streamlit_config()
st.write('')
st.write('')


with st.sidebar:
    image_url = 'https://raw.githubusercontent.com/gopiashokan/Youtube-Data-Harvesting-and-Warehousing/main/youtube_banner.JPG'
    st.image(image_url, use_column_width=True)

    option = option_menu(menu_title='', options=['Data Retrive from YouTube API', 'Store data to MongoDB',
                                                 'Migrating Data to SQL', 'Data Analysis', 'SQL Queries', 'Exit'],
                         icons=['youtube', 'database-add', 'database-fill-check', 'list-task', 'pencil-square', 'sign-turn-right-fill'])


if option == 'Data Retrive from YouTube API':

    try:

        # get input from user
        col1, col2 = st.columns(2, gap='medium')
        with col1:
            channel_id = st.text_input("Enter Channel ID: ")
        with col2:
            api_key = st.text_input("Enter Your API Key:", type='password')
        
        submit = st.button(label='Submit')

        if submit and option is not None:

            api_service_name = "youtube"
            api_version = "v3"
            youtube = googleapiclient.discovery.build(api_service_name,
                                                    api_version, developerKey=api_key)

            final = youtube_extract.main(channel_id)
            channel_name = final['channel']['channel_name']

            mongodb.drop_temp_collection()
            mongodb.data_storage(channel_name=channel_name,
                                 database='temp', 
                                 data=final)

            # display the sample data in streamlit
            st.json(youtube_extract.display_sample_data(channel_id))
            st.success('Retrived data from YouTube successfully')
            st.balloons()

    except:
        col1,col2 = st.columns([0.45,0.55])
        with col1:
            st.warning("Please enter the valid Channel ID and API key")


elif option == 'Store data to MongoDB':
    mongodb.main('project_youtube')


elif option == 'Migrating Data to SQL':
    sql.main(mdb_database='project_youtube', sql_database='youtube')


elif option == 'Data Analysis':

    s1 = sql.list_channel_names()

    if s1 == []:
        st.info("The SQL database is currently empty")

    else:
        gopi_s = psycopg2.connect(host='localhost',
                                  user='postgres',
                                  password='root',
                                  database='youtube')
        cursor = gopi_s.cursor()

        cursor.execute(f"""select channel_name 
                                   from channel 
                                   order by channel_name ASC""")

        s = cursor.fetchall()
        list_channel = ['Over All']
        list_channel.extend([i[0] for i in s])
        st.subheader('Please Select one below:')
        analysis = ['Select one', 'Channels', 'Videos']

        select_analysis = st.selectbox('', analysis)
        if select_analysis == 'Channels':
            channel_analysis.main()
        elif select_analysis == 'Videos':
            video_analysis.main()


elif option == 'SQL Queries':
    s1 = sql.list_channel_names()
    if s1 == []:
        st.info("The SQL database is currently empty")
    else:
        sql_queries.main()


elif option == 'Exit':
    mongodb.drop_temp_collection()
    st.write('')
    st.write('')
    st.success('Thank you for your time. Exiting the application')
    st.balloons()
