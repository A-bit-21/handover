import json
import requests
import base64
import os

JIRA_SECRET = os.environ['JIRA_SECRET']
JIRA_CLIENT_ID = os.environ['JIRA_CLIENT_ID']
GOOGLE_CLIENT_ID = os.environ['GOOGLE_CLIENT_ID']
GOOGLE_CLIENT_SECRET = os.environ['GOOGLE_CLIENT_SECRET']
GOOGLE_PROJECT_KEY = os.environ['GOOGLE_PROJECT_KEY']
GOOGLE_REFRESH_TOKEN = os.environ['GOOGLE_REFRESH_TOKEN']
JUMPCLOUD_KEY = os.environ['JUMPCLOUD_KEY']
ZOOM_CLIENT_ID = os.environ['ZOOM_CLIENT_ID']
ZOOM_SECRET = os.environ['ZOOM_SECRET']
ZOOM_ACCOUNT = os.environ['ZOOM_ACCOUNT']

POST_RAW_PATH = "/inboundOffboarding"
EVENT_ACTION = "employee.inactivated"

def lambda_handler(event, context):
    
    de = json.loads(event['body'])
    eventCheck = de['type']
    
    print("STAGE 1: Inbound Data Received from bob hook, Testing statement")
    
    if (event['rawPath'] == POST_RAW_PATH) and (eventCheck == EVENT_ACTION):
        
        #GET EVENT DATA FROM INBOUND BOB WEBHOOK
        
        eventType = de['type']
        timeStamp = de['creationDate']
        first = de['employee']['firstName']
        last = de['employee']['surname']
        display = de['employee']['displayName']
        email = de['employee']['email']
        eid = de['employee']['id']
        country = de['employee']['site']
        changeby = de['changedBy']['displayName']
        
        print("STAGE 2: Create Jira Tikcet")
        
        #SET THE HEADERS AND REQUEST DATA FOR JIRA CREATE TICKET
        
        auth_str = f"{JIRA_CLIENT_ID}:{JIRA_SECRET}"
        jiraEncode = base64.b64encode(auth_str.encode("ISO-8859-1")).decode("ascii")
        jiraUrl = "https://hipagesgroup.atlassian.net/rest/api/2/issue/"
        jiraHeader = {
            'Authorization': 'Basic ' + jiraEncode,
            'Content-Type': 'application/json'
        }
        
        #CREATE THE BODY OF THE REQUEST FOR JIRA
        
        jiraBody = {
            "fields": {
                "project":
                {
                  "key": "HEL"
                },
               "summary": "AOFF - Offboarding Request " + display,
               "description": "An offboarding request was received for " + display + ". The user was made inactive in bob by " + changeby + " . at:  " + timeStamp,
               "issuetype": {
                  "name": "bob-Offboarding"
               },
               "customfield_15988": eventType,
               "customfield_15981": first,
               "customfield_15982": last,
               "customfield_15986": email,
               "customfield_15983": eid,
               "customfield_15984": country,
               "customfield_15985": changeby,
               "customfield_15987": timeStamp
            }
        }
        
        createIssue = requests.post(jiraUrl, data=json.dumps(jiraBody), headers=jiraHeader)
        ctrCode = createIssue.status_code
        
        #TEST THE RESPONSE CODE RETURNED FROM THE TICKET REQUEST
        
        if ctrCode == 201:
            print("Issue Created Succesfully")
            jrReturn = createIssue.json()
            tid = jrReturn['id']
            tkey = jrReturn['key']
            turl = jrReturn['self']
            
        else:
            print("There was a problem creating the issue")
        
        print("STAGE 3: LOCK Google Account")
        
        #TEST THE USERS DOMAIN IF IT CAN BE PROCESSED WITHIN OUR GOOGLE WORKSPACE
        
        checkDomain = email.split("@")
        domain = checkDomain[1]
        
        if domain == "hipagesgroup.com.au":
            print("The users domain is within our google workspace, starting the account update process.")
            
            #GOOGLE TOKEN REQUEST
            
            tokenUrl = "https://oauth2.googleapis.com/token"
                
            #HEADERS FOR THE TOKEN REFRESH
            
            googleKeys = {
                'client_id': GOOGLE_CLIENT_ID,
                'client_secret': GOOGLE_CLIENT_SECRET,
                'refresh_token': GOOGLE_REFRESH_TOKEN,
                'grant_type': 'refresh_token'
            }
            
            #SEND THE REQUEST TO GET ACCESS TOKEN
            
            token_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            token_response = requests.post(tokenUrl, data=googleKeys, headers=token_headers)
            
            #RETREIVE THE TOKEN REQUEST DATA
                
            token_rsp_json = token_response.json()
            token_type = token_rsp_json['token_type']
            access_token = token_rsp_json['access_token']
            scope = token_rsp_json['scope']
            
            #SET THE DATA TO SUSPEND THE USER IN GOOGLE
            
            gbaseURL = "https://admin.googleapis.com/admin/directory/v1/users/"
            sendURL = gbaseURL + email + GOOGLE_PROJECT_KEY
            
            #GOOGLE SUSPEND USER HEADER
            
            sendHeaders = {
                "Authorization": f"{token_type} {access_token}",
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
            
            #GOOGLE SUSPEND USER BODY
            
            sendbody = {
                "suspended": True,
                "orgUnitPath": "/02.Deprovision",
                "includeInGlobalAddressList": False
            }
            
            #MAKE THE REQUEST TO GOOGLE
                
            suspendAction = requests.put(sendURL, data=json.dumps(sendbody), headers=sendHeaders)
            suspendStat = suspendAction.status_code
            print("Google Account Process Completed")
            
            print("STAGE 4: LOCK JumpCloud account")
        
            #SETTING THE JUMPOCLOUD ACCOUNT TO LOCKED
            
            fbaseUrl = "https://console.jumpcloud.com/api/systemusers/?filter=email:$eq:"
            fsendUrl = fbaseUrl+email
                    
            find_Header = {

                'Content-Type': 'application/json',
                'x-api-key': JUMPCLOUD_KEY
            }
            
            #SEARCH FOR THE USER IN JUMPCLOUD
            
            find_Return = requests.get(fsendUrl, headers=find_Header).json()
            userId = find_Return['results'][0]['_id']
            
            #HEADER OF THE JUMPCLOUD SUSPEND USER REQUEST
            
            suspend_header = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'x-api-key': JUMPCLOUD_KEY
            }
            
            #BODY OF THE SUSPEND REQUEST TO JUMPCLOUD
            
            suspend_body = {
                "suspended": True,
                "account_locked": True,
                "activated": False
            }
            
            sbaseUrl = "https://console.jumpcloud.com/api/systemusers/"
            suspend_Url = sbaseUrl+userId
            suspend_Request = requests.put(suspend_Url, data=json.dumps(suspend_body), headers=suspend_header)
            
            #STATUS CODE OF THE REQUEST
            
            suspendCode = suspend_Request.status_code
            print(suspendCode)
            
            print("JumpCloud Account Process Completed")
            
            print("STAGE 5: Delete the users Zoom Account")
            
            ZOOM_AUTH_URL = "https://zoom.us/oauth/token"
            
            #Set Zoom credentials encoding
            
            zoomEncode = base64.b64encode(bytes(ZOOM_CLIENT_ID + ":" + ZOOM_SECRET, "ISO-8859-1")).decode("ascii")
            
            zoomAuthHeader = {
                'Host': 'zoom.us',
                'Authorization': 'Basic ' + zoomEncode,
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            zoomTokenBody = {
                'grant_type': 'account_credentials',
                'account_id': ZOOM_ACCOUNT
            }
            
            tokenRequest = requests.post(ZOOM_AUTH_URL, data=zoomTokenBody, headers=zoomAuthHeader)
            tokenJson = tokenRequest.json()
            
            zoomAccessToken = tokenJson['access_token']
            
            zoomFindheader = {
                'Host': 'api.zoom.us',
                'Authorization': 'Bearer ' + zoomAccessToken,
                'Accept': 'application/json'
            }
            
            finduser = email
            
            zoomFindBaseUrl = "https://api.zoom.us/v2/users/"
            zoomFindSendUrl = zoomFindBaseUrl+finduser
            
            zoomSendFind = requests.get(zoomFindSendUrl, headers=zoomFindheader)
            zfReturn = zoomSendFind.json()
            
            uid = zfReturn['id']
            
            print(uid)
            
            zoomAuthHeader2 = {
                'Host': 'zoom.us',
                'Authorization': 'Basic ' + zoomEncode,
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            zoomTokenBody2 = {
                'grant_type': 'account_credentials',
                'account_id': ZOOM_ACCOUNT
            }
            
            tokenRequest2 = requests.post(ZOOM_AUTH_URL, data=zoomTokenBody2, headers=zoomAuthHeader2)
            tokenJson2 = tokenRequest.json()
            
            zoomAccessToken2 = tokenJson2['access_token']
            
            
            zoomDeleteheader = {
                'Host': 'api.zoom.us',
                'Authorization': 'Bearer ' + zoomAccessToken2,
                'Accept': 'application/json'
            }
            
            zoomDeleteBaseUrl = "https://api.zoom.us/v2/users/"
            zoomDeleteEnd = "?action=delete"
            zoomDeleteSend = zoomDeleteBaseUrl + uid + zoomDeleteEnd
            
            zoomDeleteAction = requests.delete(zoomDeleteSend, headers=zoomDeleteheader)
            zdACode = zoomDeleteAction.status_code
            
            print(zdACode)
            
            if zdACode == 204:
                print("User was successfully deleted")
            else:
                print("There was an issue delete the users zoom account")
    
        else:
            print("The users workspace domain is not in our environment - skipping Google Lock process")

    
    else:
        print("Event did not meet conditions to run")
    
