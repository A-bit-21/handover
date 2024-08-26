import duckdb
import requests
import json
import base64
import pandas as pd
import Gen_keys #Keys File
import datetime

# Defines the Function to fetch the users/Agents id's
def fetch_users_data():
    #Sets the Genesys Environment
    environment = "mypurecloud.com.au"

    #Sets the Genesys Client ID and Client Secret from the Gen_keys file
    client_id = Gen_keys.CLIENT_ID
    client_secret = Gen_keys.CLIENT_SECRET

    #Encode the client ID and client secret using Base64 encoding and decode the result as ASCII
    genesys_base_64 = base64.b64encode(bytes(client_id + ":" + client_secret, "utf-8")).decode("ascii")

    #Create the headers and body for the token api request, including the grant_type and
    # client_id and client_secret values.
    genesys_token_headers = {
        'Authorization': f"Basic {genesys_base_64}",
        'Content-Type': "application/x-www-form-urlencoded"
    }

    genesys_token_body = {
        'grant_type': "client_credentials"
    }

    #Make the request to Genesys to generate the access token for subsequent requests.
    request_token = requests.post(f"https://login.{environment}/oauth/token", headers=genesys_token_headers,
                                  data=genesys_token_body).json()
    #Retrieve the access token and token type from the response and return them as a string.
    access_token = request_token["access_token"]
    token_type = request_token["token_type"]

    #Define the headers and body for the get users request, including the access token and token type.
    get_users_header = {
        'Authorization': f"{token_type} {access_token}",
        'Content-Type': 'application/json'
    }

    #Make the request to Genesys to retrieve the list of users.
    get_users_request = requests.get(
        "https://api.mypurecloud.com.au/api/v2/users?pageSize=300&pageNumber=1&state=active",
        headers=get_users_header).json()

    #Extract the list of users from the response and store them in a list.
    users_list = get_users_request['entities']

    #Prepare the data for use and convert it to a JSON string.
    data_dump = json.dumps(users_list, indent=2, sort_keys=True)
    dj = json.loads(data_dump)

    column1_data = []
    column2_data = []
    column3_data = []

    #Loop through the list of users and extract the id, name and email and store them in a list.
    for u in range(len(dj)):
        column1_data.append(dj[u]['id'])
        column2_data.append(dj[u]['name'])
        column3_data.append(dj[u]['email'])

    users_data = {
        "user_id": column1_data,
        "name": column2_data,
        "email": column3_data
    }
    #Creates the users_df dataframe and returns it for use
    users_df = pd.DataFrame(users_data)
    return users_df

#Defines the Function to fetch the call data for each user and save it to a .csv file.
def fetch_calls_data():
    # Construct a timestamp and convert this from Sydney Local Time to the equivalent UTC time period
    # (Yesterday) This is used in the Genesys aggregation query to retreive the data from a set period.
    sydney_offset = datetime.timedelta(hours=11)
    current_utc_time = datetime.datetime.utcnow()
    current_sydney_time = current_utc_time + sydney_offset
    one_day_ago_sydney = current_sydney_time - datetime.timedelta(days=2)
    end_time_date = current_sydney_time - datetime.timedelta(days=1)
    formatted_one_day_ago = one_day_ago_sydney.strftime('%Y-%m-%d')
    formatted_end_date = end_time_date.strftime('%Y-%m-%d')
    time_construct = 'T14:00:00.000Z'
    time_construct_end = 'T12:59:00.000Z'
    start_datetime = formatted_one_day_ago + time_construct
    end_dateTime = formatted_end_date + time_construct_end
    query_date = start_datetime + "/" + end_dateTime
    #query_date = "2024-02-09T14:00:00.000Z/2024-02-10T12:59:00.000Z"
    print(query_date)

    #Call The Authentication Function to retrieve the authentication token for the Genesys Cloud API.
    environment = "mypurecloud.com.au"
    client_id = Gen_keys.CLIENT_ID
    client_secret = Gen_keys.CLIENT_SECRET

    #Encode the client ID and client secret using Base64 encoding and decode the result as ASCII.
    genesys_base_64 = base64.b64encode(bytes(client_id + ":" + client_secret, "utf-8")).decode("ascii")

    #Create the headers and body for the token api request, including the grant_type and client_id
    # and client_secret values.
    genesys_token_headers = {
        'Authorization': f"Basic {genesys_base_64}",
        'Content-Type': "application/x-www-form-urlencoded"
    }

    genesys_token_body = {
        'grant_type': "client_credentials"
    }
    #Make the request to Genesys to generate the access token for subsequent requests.
    request_token = requests.post(f"https://login.{environment}/oauth/token", headers=genesys_token_headers,
                                  data=genesys_token_body).json()
    #Retrieve the access token and token type from the response and return them as a string.
    access_token = request_token["access_token"]
    token_type = request_token["token_type"]

    #Define the headers and body for the post outbound request, including the access token and token type.
    post_outbound_header = {
        'Authorization': f"{token_type} {access_token}",
        'Content-Type': 'application/json'
    }

    #Set the body of the post outbound request, including the query date, granularity, group by,
    # metrics, and filter.
    post_outbound_body = {
        "interval": query_date,
        "granularity": "PT12H",
        "groupBy": [
            "conversationId",
            "queueId",
            "userId",
            "wrapUpCode"
        ],
        "metrics": [
            "tTalkComplete",
            "tAcw",
            "tHandle"
        ],
        "filter": {
            "type": "and",
            "predicates": [
                {
                    "dimension": "mediaType",
                    "value": "voice"
                }
            ]
        },
        "flattenMultivaluedDimensions": True,
        "alternateTimeDimension": "eventTime"
    }

    #Make the request to Genesys to retrieve the data for the required time period.
    post_outbound_request = requests.post(
        "https://api.mypurecloud.com.au/api/v2/analytics/conversations/aggregates/query", headers=post_outbound_header,
        data=json.dumps(post_outbound_body)).json()

    #Extract the returned data from the request and prepare the data for use.
    outbound_list = post_outbound_request['results']

    #Format and prepare the data for use. This is used to convert the returned data from the request
    # to a JSON string. This is then used to convert the JSON string to a Python dictionary.
    # This is then used to extract the required data from the dictionary.
    # This is then used to store the data in a list. This list is
    dv = json.dumps(outbound_list, indent=2, sort_keys=True)
    data = json.loads(dv)

    data_list = []

    # Loop through the returned data and extract the required data and store it in a list.
    # This list is then used to create a Pandas DataFrame.
    # This DataFrame is then used to save the data to a .csv file.
    for item in data:
        group_data = item.get('group', {})

        # Check and replace missing queueId and wrapUpCode with placeholder strings
        queue_id = group_data.get('queueId', 'No Queue Present')
        wrap_up_code = group_data.get('wrapUpCode', 'No Wrap up Present')

        # Retrieve other available data
        conversation_id = group_data.get('conversationId')
        media_type = group_data.get('mediaType')
        user_id = group_data.get('userId')

        interval_data = item.get('data', [])
        for interval_item in interval_data:
            interval = interval_item.get('interval')
            metrics = interval_item.get('metrics', [])

            # Initialize dictionary for metrics
            metrics_dict = {'tAcw': 0, 'tHandle': 0, 'tTalkComplete': 0}

            for metric_info in metrics:
                metric_name = metric_info.get('metric')
                sum_value_ms = metric_info['stats'].get('sum', 0)  # Time in milliseconds

                # Convert milliseconds to hours, minutes, seconds
                sec = (sum_value_ms / 1000)
                conv_sec = int(sec)
                seconds = conv_sec % (24 * 3600)
                hour = seconds // 3600
                seconds %= 3600
                minutes = seconds // 60
                seconds %= 60
                formatted_time = ("%02d:%02d:%02d" % (hour, minutes, seconds))

                metrics_dict[metric_name] = formatted_time
            # Append data to data_list list. This list is then used to create a Pandas DataFrame.
            # This DataFrame is then used to save the data to a .csv file.
            data_list.append({
                'ConversationId': conversation_id,
                'MediaType': media_type,
                'QueueId': queue_id,
                'user_id': user_id,
                'WrapUpCode': wrap_up_code,
                'Interval': interval,
                **metrics_dict  # Unpack metrics_dict into the data_list dictionary
            })

    # Create a Pandas DataFrame
    calls_df = pd.DataFrame(data_list)
    return calls_df

#fetch the dataframes
users_df = fetch_users_data()
calls_df = fetch_calls_data()

#merge the the frames on the UserId Field
merged_df = pd.merge(calls_df, users_df, on='user_id', how='left')

# Calculate summary statistics summing the tTalkTime, tAcw, and tHandle columns for each user.
summary_df = merged_df.groupby(['name', 'email']).agg({
    'user_id': 'count',  # Total Calls made
    'tTalkComplete': lambda x: str(datetime.timedelta(seconds=int(pd.to_timedelta(x).dt.total_seconds().sum()))),  # Total Talk time
    'tAcw': lambda x: str(datetime.timedelta(seconds=int(pd.to_timedelta(x).dt.total_seconds().sum())))  # Total ACW time
}).rename(columns={
    'user_id': 'Total Calls',
    'tTalkComplete': 'Total Talk',
    'tAcw': 'Total ACW'
}).reset_index()

summary_df['Average Talk'] = pd.to_timedelta(summary_df['Total Talk']).dt.total_seconds() / summary_df['Total Calls']
summary_df['Average ACW'] = pd.to_timedelta(summary_df['Total ACW']).dt.total_seconds() / summary_df['Total Calls']
summary_df['Call Cadence'] = round(summary_df['Total Calls'] / 7, 2)  # Assuming 7hr working day

summary_df['Average Talk'] = summary_df['Average Talk'].apply(lambda x: '{:02}:{:02}:{:02}'.format(int(x // 3600), int((x % 3600) // 60), int(x % 60)))
summary_df['Average ACW'] = summary_df['Average ACW'].apply(lambda x: '{:02}:{:02}:{:02}'.format(int(x // 3600), int((x % 3600) // 60), int(x % 60)))

set_current_utc_time = datetime.datetime.now()
end_time_date = (set_current_utc_time - datetime.timedelta(days=1))
row_date = end_time_date.strftime("%Y-%m-%d")
#row_date = "2024-02-10"
reporting_month = end_time_date.strftime("%B")
#reporting_month = "February"
summary_df['Date'] = row_date
summary_df['Reporting Month'] = reporting_month
columns = ['Reporting Month', 'Date', 'name', 'email', 'Total Calls', 'Total Talk', 'Total ACW', 'Average Talk', 'Average ACW', 'Call Cadence']
summary_df = summary_df.reindex(columns=columns)

#Old : New
new_column_names = {
    'Reporting Month': 'Reporting_Month',
    'Date': 'Date',
    'name': 'Agent',
    'email': 'Email',
    'Total Calls': 'Total_Calls',
    'Total Talk': 'Total_Talk',
    'Total ACW': 'Total_ACW',
    'Average Talk': 'Average_Talk',
    'Average ACW': 'Average_ACW',
    'Call Cadence': 'Call_Cadence'
}
new_report_df = summary_df.rename(columns=new_column_names)
new_report_df.reindex(columns=new_column_names)

#create connection to duck.db
conn = duckdb.connect(database='quack.db')
#insert command to insert the data into the table
conn.sql("INSERT INTO agent_summary SELECT * FROM new_report_df")
#commit the changes and close the database connection
conn.commit()
conn.close()
print("Data has been inserted into the agent_Summary table")
