# YouTube Data Harvesting and Warehousing using SQL, MongoDB, and Streamlit

**Introduction**

YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable information from YouTube channels. The extracted data is then stored in a MongoDB database, subsequently migrated to a SQL data warehouse, and made accessible for analysis and exploration within the Streamlit app.

**Table of Contents**

Installation
Usage
Features
Retrieving data from the YouTube API
Storing data in MongoDB
Migrating data to a SQL data warehouse
Data Analysis
Contributing
License
Documentation
Acknowledgments
Troubleshooting
Contact

**Installation**

To run this project, you need to install the following packages:
```python
import googleapiclient.discovery
import pymongo
import pandas as pd
import psycopg2
import datetime
import streamlit as st
import plotly.express as px
```

**Usage**

To use this project, follow these steps:

1. Clone the repository: **git clone https://github.com/gopiashokan/Youtube-Harvesting-and-Warehousing.git**
2. Install the required packages: **pip install -r requirements.txt**
3. Run the Streamlit app: **streamlit run app.py**
4. Access the app in your browser at **http://localhost:8501**

**Features**

1. Retrieve data from the YouTube API, including channel information, playlists, videos, and comments.
2. Store the retrieved data in a MongoDB database.
3. Migrate the data to a SQL data warehouse.
4. Analyze and visualize data using Streamlit and Plotly.
5. Perform queries on the SQL data warehouse.
6. Gain insights into channel performance, video metrics, and more.

**Retrieving data from the YouTube API**

The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments. By interacting with the Google API, we collect the data and merge it into a structured JSON file.

**Storing data in MongoDB**

The retrieved data is automatically stored in a temporary MongoDB database. Once stored, the data is transferred to the main database. If data already exists, it can be overwritten with user confirmation. This storage process ensures efficient data management and preservation.

**Migrating data to a SQL data warehouse**

The application allows users to migrate data from MongoDB to a SQL data warehouse. Users can choose which channel's data to migrate. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, playlists, videos, and comments, utilizing SQL queries.

**Data Analysis**

The project provides comprehensive data analysis capabilities for both channels and videos. Users can easily visualize and analyze the collected data within the Streamlit app.

**Channel Analysis**

Channel analysis includes insights on playlists, videos, subscribers, views, likes, comments, and durations. Gain a deep understanding of the channel's performance and audience engagement through detailed visualizations and summaries.

**Video Analysis**

Video analysis focuses on views, likes, comments, and durations, enabling both overall channel and specific channel perspectives. Leverage visual representations and metrics to extract valuable insights from individual videos.

**Contributing**

Contributions to this project are welcome! If you encounter any issues or have suggestions for improvements, please feel free to submit a pull request.

**License**

This project is licensed under the MIT License. Please review the LICENSE file for more details.

**Contact**
For any further questions or inquiries, please contact us at gopiashokankiot@gmail.com. We are happy to assist you.
