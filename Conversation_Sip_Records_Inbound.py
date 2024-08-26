import logging
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

# Define a function to retrieve the conversation ids for the specified time period and save them to a .csv file.
def get_conversation_ids(auth_token):
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
    print(query_date)

    environment = "mypurecloud.com.au"

    # Call The Authentication Function to retrieve the authentication token for the Genesys Cloud API.
    post_outbound_header = {
        'Authorization': auth_token,
        'Content-Type': 'application/json'
    }

    #Set the Body for the Genesys Query, including the required time period, direction inbound. Values retreived:  conversationId
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
                    "dimension": "direction",
                    "value": "inbound"
                }
            ]
        },
        "flattenMultivaluedDimensions": True,
        "alternateTimeDimension": "eventTime"
    }

    # Make the request  to Genesys to retrieve the data for the required time period.
    post_outbound_request = requests.post(
        "https://api.mypurecloud.com.au/api/v2/analytics/conversations/aggregates/query",
        headers=post_outbound_header,
        data=json.dumps(post_outbound_body)).json()

    # Extract the returned data from the request
    outbound_list = post_outbound_request['results']

    # Prepare the Data for use
    dv = json.dumps(outbound_list, indent=2, sort_keys=True)
    data = json.loads(dv)

    data_list = []

    #Loop through the returned data and extract the conversationId and save it to a list. This list is then used to retrieve the SIP records for each conversationId.
    for item in data:
        group_data = item.get('group', {})
        conversation_id = group_data.get('conversationId')
        data_list.append(conversation_id)

    conversations_df = pd.DataFrame(data_list, columns=['ConversationID'])
    print(conversations_df)
    return conversations_df

# Define a function to retrieve the SIP records for each conversationId and save them to a .csv file.
def get_sip_records(auth_token, conversations_df):
    environment = "mypurecloud.com.au"

    # Set the list of conversationIds from the conversations_df DataFrame. This list is used to retrieve the SIP records for each conversationId.
    conversation_ids = conversations_df['ConversationID'].tolist()

    #Create a for loop to iterate through the conversationIds and retrieve the SIP records for each conversationId.
    data_list = []
    for cid in conversation_ids:

        # Set the URL for the Genesys Cloud API to retrieve the SIP records for the specified conversationId. This will return a JSON response.
        post_url = f"https://api.mypurecloud.com.au/api/v2/telephony/siptraces?conversationId={cid}"

        # Set the headers and options for the request to the Genesys Cloud API. This includes the authentication token and the content type.
        options = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': auth_token
        }

        # Make the request to the Genesys Cloud API to retrieve the SIP records for the specified conversationId. This will return a JSON response.
        try:
            response = requests.get(post_url, headers=options)
            response.raise_for_status()  # Raise an error for non-200 status codes

            json_data = response.json()
            array_count = json_data['count']

            # check if there is any data returned from the API. If not, append a placeholder value. If data is returned, extract the relevant data and append it to the data_list.
            if array_count > 0:
                filter_id = json_data['data'][0]['id']
                from_dnis = json_data['data'][0]['fromUser']
                to_user = json_data['data'][0]['toUser']
                call_id = json_data['data'][0]['callid']
                sip_direction = "inbound"

                # Append relevant data to data_list
                data_list.append({
                    'ConversationID': cid,
                    'FilterID': filter_id,
                    'FromUser': from_dnis,
                    'ToUser': to_user,
                    'CallID': call_id,
                    'Direction': sip_direction
                })
            else:
                no_direction = "inbound"
                no_data = "Did Not Connect"
                data_list.append({
                    'ConversationID': cid,
                    'FilterID': no_data,
                    'FromUser': "-",
                    'ToUser': "-",
                    'CallID': "-",
                    'Direction': no_direction
                })
        except requests.exceptions.RequestException as e:
            print(f"Request failed for conversation ID {cid}: {e}")
            # Append placeholder data for the failed request
            data_list.append({
                'ConversationID': cid,
                'FilterID': "Request Failed",
                'FromUser': "-",
                'ToUser': "-",
                'CallID': "-",
                'Direction': "inbound"
            })

    # Create a DataFrame from collected data
    sip_records_df = pd.DataFrame(data_list)
    return sip_records_df

def main():
    auth_token = retrieve_auth_token()
    conversation_df = get_conversation_ids(auth_token)
    sip_records_df = get_sip_records(auth_token, conversation_df)
    print(sip_records_df)
    # create connection to duck.db
    conn = duckdb.connect(database='quack.db')
    # insert command to insert the data into the table
    conn.sql("INSERT INTO sips SELECT * FROM sip_records_df")
    # commit the changes and close the database connection
    conn.commit()
    conn.close()
    print("Data has been inserted into the Sip Records table")

if __name__=="__main__":
    main()
