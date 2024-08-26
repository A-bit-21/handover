#Set the required Python packages for use with the script
import requests
import json
import base64
import pandas as pd
import datetime
import Gen_keys
import duckdb

# Define a function to retrieve the authentication token for the Genesys Cloud API
def retrieve_auth_token():
    # Set the Genesys Cloud environment and client ID and client secret
    environment = "mypurecloud.com.au"
    client_id = Gen_keys.CLIENT_ID
    client_secret = Gen_keys.CLIENT_SECRET

    # Encode the client ID and client secret using Base64 encoding and decode the result as ASCII
    genesys_base_64 = base64.b64encode(bytes(client_id + ":" + client_secret, "utf-8")).decode("ascii")

    # Create the headers and body for the token api request, including the grant_type and client_id and client_secret values.
    genesys_token_headers = {
        'Authorization': f"Basic {genesys_base_64}",
        'Content-Type': "application/x-www-form-urlencoded"
    }

    genesys_token_body = {
        'grant_type': "client_credentials"
    }

    # Make the request to Genesys to generate the access token for subsequent requests.
    request_token = requests.post(f"https://login.{environment}/oauth/token", headers=genesys_token_headers,
                                  data=genesys_token_body).json()

    # Retrieve the access token and token type from the response and return them as a string.
    access_token = request_token["access_token"]
    token_type = request_token["token_type"]

    return f"{token_type} {access_token}"


def fetch_users():
    #Sets the Genesys Environment
    environment = "mypurecloud.com.au"

    #Sets the Genesys Client ID and Client Secret from the Gen_keys file
    client_id = Gen_keys.CLIENT_ID
    client_secret = Gen_keys.CLIENT_SECRET

    #Encode the client ID and client secret using Base64 encoding and decode the result as ASCII
    genesys_base_64 = base64.b64encode(bytes(client_id + ":" + client_secret, "utf-8")).decode("ascii")

    #Create the headers and body for the token api request, including the grant_type and client_id and client_secret values.
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

    #Loop through the list of users and extract the id, name and email and store them in a list.
    for u in range(len(dj)):
        column1_data.append(dj[u]['id'])
        column2_data.append(dj[u]['name'])

    users_data = {
        "user_id": column1_data,
        "name": column2_data
    }
    #Creates the users_df dataframe and returns it for use
    users_df = pd.DataFrame(users_data)
    return users_df


def fetch_queues():
    # set the required genesys environment
    environment = "mypurecloud.com.au"

    # Set the genesys access keys
    client_id = Gen_keys.CLIENT_ID
    client_secret = Gen_keys.CLIENT_SECRET

    # Create the required Base64 enconding for the token authorization access request to genesys
    genesys_base_64 = base64.b64encode(bytes(client_id + ":" + client_secret, "utf-8")).decode("ascii")

    # Set the Genesys Headers for the Token api request, including the base64 encode
    genesys_token_headers = {
        'Authorization': f"Basic {genesys_base_64}",
        'Content-Type': "application/x-www-form-urlencoded"
    }

    # Set the Genesys Body for the Token api request, including the grant_type and client_id and client_secret values.
    genesys_token_body = {
        'grant_type': "client_credentials"
    }

    # Make the request to Genesys to generate the access token for subsequent requests
    request_token = requests.post(f"https://login.{environment}/oauth/token", headers=genesys_token_headers,
                                  data=genesys_token_body).json()

    # Retreive the Access token and token type to pass into the subsequent api request
    access_token = request_token["access_token"]
    token_type = request_token["token_type"]

    # Set the Genesys Headers for the Queues api request, including the access token and content type.
    get_queues_header = {
        'Authorization': f"{token_type} {access_token}",
        'Content-Type': 'application/json'
    }

    # Make the request to Genesys to get the queues information.
    get_queues_request = requests.get(
        "https://api.mypurecloud.com.au/api/v2/routing/queues?pageNumber=1&pageSize=100&sortOrder=asc&hasPeer=false",
        headers=get_queues_header).json()

    # Retreive the Data from the request
    queues_list = get_queues_request['entities']

    # Format and prepare the data for use
    data_dump = json.dumps(queues_list, indent=2, sort_keys=True)
    dj = json.loads(data_dump)

    column1_data = []
    column2_data = []

    # Loop through all the available data and retreive and append the queue ID, Name, total Members
    for q in range(len(dj)):
        column1_data.append(dj[q]['id'])
        column2_data.append(dj[q]['name'])

    # prepare the retreived data to place into a pandas dataframe
    data = {
        "queue_id": column1_data,
        "queue_name": column2_data
    }
    # Place the data into a queues_df dataframe
    queues_df = pd.DataFrame(data)
    return queues_df


def fetch_wrapup():
    # set the required genesys environment
    environment = "mypurecloud.com.au"

    # Set the genesys access keys
    client_id = Gen_keys.CLIENT_ID
    client_secret = Gen_keys.CLIENT_SECRET

    # Create the required Base64 enconding for the token authorization access request to genesys
    genesys_base_64 = base64.b64encode(bytes(client_id + ":" + client_secret, "utf-8")).decode("ascii")

    # Set the Genesys Headers for the Token api request, including the base64 encode
    genesys_token_headers = {
        'Authorization': f"Basic {genesys_base_64}",
        'Content-Type': "application/x-www-form-urlencoded"
    }

    # Set the Genesys Body for the Token api request, including the grant_type and client_id and client_secret values.
    genesys_token_body = {
        'grant_type': "client_credentials"
    }

    # Make the request to Genesys to generate the access token for subsequent requests
    request_token = requests.post(f"https://login.{environment}/oauth/token", headers=genesys_token_headers,
                                  data=genesys_token_body).json()

    # Retreive the Access token and token type to pass into the subsequent api request
    access_token = request_token["access_token"]
    token_type = request_token["token_type"]

    # Set the Genesys Headers for the wrap up codes api request, including the access token and content type.
    get_wrap_ups_header = {
        'Authorization': f"{token_type} {access_token}",
        'Content-Type': 'application/json'
    }

    # Make the first request to Genesys to get the wrap-up codes information.
    get_wrap_ups_request_page1 = requests.get(
        "https://api.mypurecloud.com.au/api/v2/routing/wrapupcodes?pageSize=100&pageNumber=1&sortBy=name&sortOrder=ascending",
        headers=get_wrap_ups_header).json()
    # Retreive the Data from the request
    wrap_ups_page_1 = get_wrap_ups_request_page1['entities']

    # Make the Second request to Genesys to get the wrap-up codes information.
    get_wrap_ups_request_page2 = requests.get(
        "https://api.mypurecloud.com.au/api/v2/routing/wrapupcodes?pageSize=100&pageNumber=2&sortBy=name&sortOrder=ascending",
        headers=get_wrap_ups_header).json()
    # Retreive the Data from the request
    wrap_ups_page_2 = get_wrap_ups_request_page2['entities']

    # Combine the wrap_ups_page_1 and wrap_ups_page_2 into one list.
    wrap_ups_list = wrap_ups_page_1 + wrap_ups_page_2

    # Format and prepare the data for use
    data_dump = json.dumps(wrap_ups_list, indent=2, sort_keys=True)
    dj = json.loads(data_dump)

    column1_data = []
    column2_data = []

    # Loop through all the available data and retreive and append the wrap-up Id and name
    for w in range(len(dj)):
        column1_data.append(dj[w]['id'])
        column2_data.append(dj[w]['name'])

    # prepare the retrieved data to place into a pandas dataframe
    data = {
        "wrap_up_id": column1_data,
        "wrap_up_name": column2_data
    }

    # Place the data into wrap_ups_df dataframe
    wrap_ups_df = pd.DataFrame(data)
    return wrap_ups_df


def fetch_call_summary():
    # Construct a timestamp and convert this from Sydney Local Time to the equivalent UTC time period (Yesterday) This is used in the Genesys aggregation query to retreive the data from a set period.
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
    #query_date = "2023-10-31T14:00:00.000Z/2023-11-01T12:59:00.000Z"
    print(query_date)

    # set the required genesys environment
    environment = "mypurecloud.com.au"

    # Set the genesys access keys
    client_id = Gen_keys.CLIENT_ID
    client_secret = Gen_keys.CLIENT_SECRET

    # Create the required Base64 enconding for the token authorization access request to genesys
    genesys_base_64 = base64.b64encode(bytes(client_id + ":" + client_secret, "utf-8")).decode("ascii")

    # Set the Genesys Headers for the Token api request, including the base64 encode
    genesys_token_headers = {
        'Authorization': f"Basic {genesys_base_64}",
        'Content-Type': "application/x-www-form-urlencoded"
    }

    # Set the Genesys Body for the Token api request, including the grant_type and client_id and client_secret values.
    genesys_token_body = {
        'grant_type': "client_credentials"
    }

    # Make the request to Genesys to generate the access token for subsequent requests
    request_token = requests.post(f"https://login.{environment}/oauth/token", headers=genesys_token_headers,
                                  data=genesys_token_body).json()

    # Retreive the Access token and token type to pass into the subsequent api request
    access_token = request_token["access_token"]
    token_type = request_token["token_type"]

    # Set the Genesys Headers for the wrap up codes api request, including the access token and content type.
    post_outbound_header = {
        'Authorization': f"{token_type} {access_token}",
        'Content-Type': 'application/json'
    }

    # Create the required Genesys Body for the interactions aggregation query, including the required time period. Values retreived:  conversationId, queueId, userId, wrapUpCode, direction, mediaType, tTalkComplete, tAcw, tHandle, tHeldComplete.
    post_outbound_body = {
        "interval": query_date,
        "granularity": "PT12H",
        "groupBy": [
            "conversationId",
            "queueId",
            "userId",
            "wrapUpCode",
            "direction"
        ],
        "metrics": [
            "tTalkComplete",
            "tAcw",
            "tHandle",
            "tHeldComplete"
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

    # Make the request  to Genesys to retrieve the data for the required time period.
    post_outbound_request = requests.post(
        "https://api.mypurecloud.com.au/api/v2/analytics/conversations/aggregates/query", headers=post_outbound_header,
        data=json.dumps(post_outbound_body)).json()

    # Retreive the Data from the request
    outbound_list = post_outbound_request['results']

    # Format and prepare the data for use
    dv = json.dumps(outbound_list, indent=2, sort_keys=True)
    data = json.loads(dv)

    data_list = []

    for item in data:
        group_data = item.get('group', {})

        # Check and replace missing queueId and wrapUpCode with placeholder values
        queue_id = group_data.get('queueId', 'No Queue Present')
        wrap_up_code = group_data.get('wrapUpCode', 'No Wrap up Present')

        # Retrieve other data for use, conversation_Id, media_type, user_id, direction
        conversation_id = group_data.get('conversationId')
        media_type = group_data.get('mediaType')
        user_id = group_data.get('userId')
        direction = group_data.get('direction')

        # Retrieve metrics for use, tTalkComplete, tAcw, tHandle, tHeldComplete. Looking at the length of the array to determine if there is more than one metric. If there is more than one metric, the metrics will be added to the dictionary. If there is only one metric, the metric will be added to the dictionary.
        interval_data = item.get('data', [])
        for interval_item in interval_data:
            interval = interval_item.get('interval')
            metrics = interval_item.get('metrics', [])

            # Initialize dictionary for metrics
            metrics_dict = {'tAcw': 0, 'tHandle': 0, 'tTalkComplete': 0, 'tHeldComplete': 0}

            # Loop through metrics and add them to the metrics_dict dictionary. And If metric value exsists convert this to hh:mm:ss format
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

            # Append all data to the data_list list, including the conversation_Id, direction, media_type, queue_id, user_id, wrap_up_code, interval and the metrics_dict dictionary.
            data_list.append({
                'ConversationId': conversation_id,
                'Direction': direction,
                'MediaType': media_type,
                'QueueId': queue_id,
                'UserId': user_id,
                'WrapUpCode': wrap_up_code,
                'Interval': interval,
                **metrics_dict  # Unpack metrics_dict into the data_list dictionary
            })

    # Create a Pandas DataFrame
    calls_df = pd.DataFrame(data_list)
    return calls_df

#fetch the dataframes


users_df = fetch_users()
queues_df = fetch_queues()
wrap_ups_df = fetch_wrapup()
calls_df = fetch_call_summary()

#Merge the dataframes together. The users_df dataframe is merged into the calls_df dataframe on the 'UserId' column. The queues_df dataframe is merged into the calls_df dataframe on the 'QueueId' column. The wrap_ups_df dataframe is merged into the calls_df dataframe on the 'Wrapcode

# Merge users_df into calls_df on the 'user_id' column
merged_calls_df = pd.merge(calls_df, users_df, how='left', left_on='UserId', right_on='user_id')
# Drop the redundant 'user_id' column after merging
merged_calls_df.drop('user_id', axis=1, inplace=True)
# Merge queues_df into calls_df on the 'QueueId' column
merged_calls_df_with_queues = pd.merge(merged_calls_df, queues_df, how='left', left_on='QueueId', right_on='queue_id')
# Fill missing queue names with "No Queue Present"
merged_calls_df_with_queues['queue_name'].fillna('No Queue Present', inplace=True)
# Drop the redundant 'queue_id' column after merging
merged_calls_df_with_queues.drop('queue_id', axis=1, inplace=True)
# Merge wrap_ups_df into calls_df on the 'WrapUpCode' column
merged_calls_df_with_wrapups = pd.merge(merged_calls_df_with_queues, wrap_ups_df, how='left', left_on='WrapUpCode', right_on='wrap_up_id')
# Fill missing wrap-up names with "No Wrap up Present"
merged_calls_df_with_wrapups['wrap_up_name'].fillna('No Wrap up Present', inplace=True)
# Drop the redundant 'wrap_up_id' column after merging
merged_calls_df_with_wrapups.drop('wrap_up_id', axis=1, inplace=True)
# Create a new Data Frame Called report_df and Drop columns UserId, QueueId, WrapUpCode, Interval. This will be used to create the Sales report.
report_df = merged_calls_df_with_wrapups.drop(['UserId', 'QueueId', 'WrapUpCode', 'Interval'], axis=1)

#Evaluate if call was made to or from a Queue and add the results to the 'Queue Selected' column.


def queue_selected(row):
    if row['queue_name'] == 'No Queue Present':
        return 'Not Selected'
    else:
        return 'Selected'

report_df['Queue Selected'] = report_df.apply(queue_selected, axis=1)

#Evaluate if call has a wrap-up code and add the results to the 'Wrap Up Selected' column.


def wrap_up_selected(row):
    if row['wrap_up_name'] == 'No Wrap up Present':
        return 'Not Selected'
    else:
        return 'Selected'

report_df['Wrap Up Selected'] = report_df.apply(wrap_up_selected, axis=1)

#Convert 'tTalkComplete' to Timedelta and add the results to the 'Total Talk Check' column.
report_df['tTalkComplete'] = pd.to_timedelta(report_df['tTalkComplete'])

#Strip 'X Days' from the 'tTalkComplete' column.
report_df['tTalkComplete'] = report_df['tTalkComplete'].astype(str).str[-8:]

#load tHeldComplete to Timedelta and add the results to the 'Total Hold Check' column.
report_df['tHeldComplete'] = pd.to_timedelta(report_df['tHeldComplete'])

#Strip 'X Days' from the 'tHeldComplete' column.
report_df['tHeldComplete'] = report_df['tHeldComplete'].astype(str).str[-8:]

# Load & Strip 'X Days' from the 'tAcw' column.
report_df['tAcw'] = pd.to_timedelta(report_df['tAcw'])
report_df['tAcw'] = report_df['tAcw'].astype(str).str[-8:]

# Load & Strip 'X Days' from the 'tHandle' column.
report_df['tHandle'] = pd.to_timedelta(report_df['tHandle'])
report_df['tHandle'] = report_df['tHandle'].astype(str).str[-8:]

#re - order the columns
new_column_order = ['ConversationId', 'Direction', 'MediaType', 'name', 'queue_name', 'wrap_up_name', 'Queue Selected', 'Wrap Up Selected', 'tHandle', 'tTalkComplete', 'tHeldComplete', 'tAcw']
report_df = report_df.reindex(columns=new_column_order)


#rename the columns
new_column_names = {
    'ConversationId': 'ConversationID',
    'Direction': 'Direction',
    'MediaType': 'Media_Type',
    'name': 'Agent_Name',
    'queue_name': 'Queue_Name',
    'wrap_up_name': 'Wrap_Up_Code',
    'Queue Selected': 'Queue_Selected',
    'Wrap Up Selected': 'Wrap_Up_Selected',
    'tHandle': 'Total_Handle_Time',
    'tTalkComplete': 'Total_Talk_Time',
    'tHeldComplete': 'Total_Hold_Time',
    'tAcw': 'Total_ACW_Time',
}

report_df = report_df.rename(columns=new_column_names)

set_current_utc_time = datetime.datetime.now()
end_time_date = (set_current_utc_time - datetime.timedelta(days=1))
row_date = end_time_date.strftime("%Y-%m-%d")
reporting_month = end_time_date.strftime("%B")
report_df['Date'] = row_date
report_df['Reporting_Month'] = reporting_month
columns = ['Reporting_Month', 'Date', 'ConversationID', 'Direction', 'Media_Type', 'Agent_Name', 'Queue_Name', 'Wrap_Up_Code', 'Queue_Selected', 'Wrap_Up_Selected', 'Total_Handle_Time', 'Total_Talk_Time', 'Total_Hold_Time', 'Total_ACW_Time']
report_df = report_df.reindex(columns=columns)

conversations_df = report_df

#create connection to duck.db
conn = duckdb.connect(database='quack.db')
#insert command to insert the data into the table
conn.sql("INSERT INTO conversations SELECT * FROM conversations_df")
#commit the changes and close the database connection
conn.commit()
conn.close()
print("Data has been inserted into the Conversations table")
