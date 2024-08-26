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
   # query_date = "2024-02-04T14:00:00.000Z/2024-02-05T12:59:00.000Z"
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

def get_call_analytics(auth_token, conversations_df):
    environment = "mypurecloud.com.au"
    conversation_ids = conversations_df['ConversationID'].tolist()
    data_list = []
    for cid in conversation_ids:

        post_url = f"https://api.mypurecloud.com.au/api/v2/speechandtextanalytics/conversations/{cid}"
        # Set the headers and options for the request to the Genesys Cloud API. This includes the authentication token and the content type.
        options = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': auth_token
        }

        # Initialize conversation_id outside the try block
        conversation_id = None

        try:
            get_sentiment = requests.get(post_url, headers=options)
            get_sentiment.raise_for_status()
            data_dump = json.dumps(get_sentiment.json(), indent=2, sort_keys=True)
            data = json.loads(data_dump)

            if data is not None:
                conversation_id = data['conversation'].get('id', None)
                acd_duration_percentage = data['participantMetrics'].get('acdDurationPercentage', 0.0)
                agent_duration_percentage = data['participantMetrics'].get('agentDurationPercentage', 0.0)
                customer_duration_percentage = data['participantMetrics'].get('customerDurationPercentage', 0.0)
                ivr_duration_percentage = data['participantMetrics'].get('ivrDurationPercentage', 0.0)
                other_duration_percentage = data['participantMetrics'].get('otherDurationPercentage', 0.0)
                over_talk_count = data['participantMetrics'].get('overtalkCount', 0.0)
                over_talk_duration = data['participantMetrics'].get('overtalkDurationPercentage', 0.0)
                silence_duration = data['participantMetrics'].get('silenceDurationPercentage', 0.0)
                sentiment_score = data.get('sentimentScore', 0.0)
                sentiment_trend = data.get('sentimentTrend', 0.0)
                sentiment_trend_class = data.get('sentimentTrendClass', None)

                data_list.append({
                    'Conversation ID': conversation_id,
                    'ACD Duration Percentage': acd_duration_percentage,
                    'Agent Duration Percentage': agent_duration_percentage,
                    'Customer Duration Percentage': customer_duration_percentage,
                    'IVR Duration Percentage': ivr_duration_percentage,
                    'Other Duration Percentage': other_duration_percentage,
                    'Over Talk Count': over_talk_count,
                    'Over Talk Duration': over_talk_duration,
                    'Silence Duration': silence_duration,
                    'Sentiment Score': sentiment_score,
                    'Sentiment Trend': sentiment_trend,
                    'Sentiment Trend Class': sentiment_trend_class
                })
            else:
                data_list.append({
                    'Conversation ID': conversation_id,
                    'ACD Duration Percentage': 0.0,
                    'Agent Duration Percentage': 0.0,
                    'Customer Duration Percentage': 0.0,
                    'IVR Duration Percentage': 0.0,
                    'Other Duration Percentage': 0.0,
                    'Over Talk Count': 0,
                    'Over Talk Duration': 0.0,
                    'Silence Duration': 0.0,
                    'Sentiment Score': 0.0,
                    'Sentiment Trend': 0.0,
                    'Sentiment Trend Class': "No Data Recorded"
                })
        except requests.exceptions.RequestException as e:
            print(f"Request Failed for conversation ID: {cid}: with error: {e}")
            data_list.append({
                'Conversation ID': cid,
                'ACD Duration Percentage': 0.0,
                'Agent Duration Percentage': 0.0,
                'Customer Duration Percentage': 0.0,
                'IVR Duration Percentage': 0.0,
                'Other Duration Percentage': 0.0,
                'Over Talk Count': 0,
                'Over Talk Duration': 0.0,
                'Silence Duration': 0.0,
                'Sentiment Score': 0.0,
                'Sentiment Trend': 0.0,
                'Sentiment Trend Class': "No Record Found"
            })

    analysis_df = pd.DataFrame(data_list)

    new_column_names = {
        'Conversations ID': 'ConversationID',
        'ACD Duration Percentage': 'ACD_Duration_Percentage',
        'Agent Duration Percentage': 'Agent_Duration_Percentage',
        'Customer Duration Percentage': 'Customer_Duration_Percentage',
        'IVR Duration Percentage': 'IVR_Duration_Percentage',
        'Other Duration Percentage': 'Other_Duration_Percentage',
        'Over Talk Count': 'Over_Talk_Count',
        'Over Talk Duration': 'Over_Talk_Duration',
        'Silence Duration': 'Silence_Duration',
        'Sentiment Score': 'Sentiment_Score',
        'Sentiment Trend': 'Sentiment_Trend',
        'Semtiment Trend Class': 'Sentiment_Trend_Class'
    }
    analysis_df.rename(columns=new_column_names)
    analysis_df.reindex(columns=new_column_names)
    return analysis_df

def main():

    auth_token = retrieve_auth_token()
    conversations_df = get_conversation_ids(auth_token)
    anaylsis_df = get_call_analytics(auth_token, conversations_df)
    print(anaylsis_df)
    # create connection to duck.db
    conn = duckdb.connect(database='quack.db')
    # insert command to insert the data into the table
    conn.sql("INSERT INTO conversation_sentiment SELECT * FROM anaylsis_df")
    # commit the changes and close the database connection
    conn.commit()
    conn.close()
    print("Data has been inserted into the conversations_sentiment table")

if __name__ == "__main__":
    main()
