import boto3
import json
import requests
import random
from requests_aws4auth import AWS4Auth

def receiveMsgFromSqsQueue():
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/283227796002/queue-name'
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['SentTimestamp'],
        MaxNumberOfMessages=5,
        MessageAttributeNames=['All'],
        VisibilityTimeout=10,
        WaitTimeSeconds=0
        )
    return response

# The function return list of business id
def findRestaurantFromElasticSearch(cuisine):
    region = 'us-east-1'
    service = 'es'
    credentials = boto3.Session(aws_access_key_id="",
                          aws_secret_access_key="", 
                          region_name="us-east-1").get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    host = 'search-diningconcierge-7cdehhfi5s7grr3lrxzvxxzpce.us-east-1.es.amazonaws.com'
    index = 'restaurants'
    url = 'https://' + host + '/' + index + '/_search'
    # i am just getting 3 buisiness id from es but its not random rn
    query = {
        "size": 1300,
        "query": {
            "query_string": {
                "default_field": "cuisine",
                "query": cuisine
            }
        }
    }
    headers = { "Content-Type": "application/json" }
    response = requests.get(url,auth=awsauth, headers=headers, data=json.dumps(query))
    res = response.json()
    hits = res['hits']['hits']
    buisinessIds = []
    for hit in hits:
        buisinessIds.append(str(hit['_source']['RestaurantID']))
    print('len: ', len(buisinessIds))
    return buisinessIds

# function returns detail of all resturantids as a list(working)
def getRestaurantFromDb(restaurantIds):
    res = []
    client = boto3.resource('dynamodb')
    table = client.Table('yelp-restaurants')
    print('definitioin:', table.attribute_definitions)
    print('table : ' , table)
    for id in restaurantIds:
        response = table.get_item(Key={'Business ID': id})
        res.append(response)
    return res

def getMsgToSend(restaurantDetails,message):
    msg = json.loads(message['Body'])
    noOfPeople = msg['Number of people']
    time = msg['Dining Time']
    cuisine = msg['Cuisine']
    separator = ', '
    resOneName = restaurantDetails[0]['Item']['name']
    resOneAdd = separator.join(restaurantDetails[0]['Item']['address'])
    resTwoName = restaurantDetails[1]['Item']['name']
    resTwoAdd = separator.join(restaurantDetails[1]['Item']['address'])
    resThreeName = restaurantDetails[2]['Item']['name']
    resThreeAdd = separator.join(restaurantDetails[2]['Item']['address'])
    msg = 'Hello! Here are my {0} restaurant suggestions for {1} people, for {2} at : 1. {3}, located at {4}, 2. {5}, located at {6},3. {7}, located at {8}. Enjoy your meal!'.format(cuisine,noOfPeople,time,resOneName,resOneAdd,resTwoName,resTwoAdd,resThreeName,resThreeAdd)
    return msg
    
def deleteMsg(receipt_handle):
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/283227796002/queue-name'
    sqs.delete_message(QueueUrl=queue_url,
    ReceiptHandle=receipt_handle
    )

def lambda_handler(event, context):
    # getting response from sqs queue
    sqsQueueResponse = receiveMsgFromSqsQueue()
    print('SQS: ', sqsQueueResponse)
    if "Messages" in sqsQueueResponse.keys():
        for message in sqsQueueResponse['Messages']:
            msg = json.loads(message['Body'])
            cuisine = msg['Cuisine']
            restaurantIds = findRestaurantFromElasticSearch(cuisine)
            # Assume that it returns a list of restaurantsIds
            # call some random function to select 3 from the list
            restaurantIds = random.sample(restaurantIds, 3)
            print('restaurantIds :', restaurantIds)
            restaurantDetails = getRestaurantFromDb(restaurantIds)
            # now we have all required details to send the sms
            # now we will create the required message using the details
            msgToSend = getMsgToSend(restaurantDetails,message)
            print(msgToSend)
            email = msg['Email']
            temp_email(msgToSend,email)
            #now delete message from queue
            receipt_handle = message['ReceiptHandle']
            deleteMsg(receipt_handle)


def temp_email(sendMessage,email):
    ses_client = boto3.client("ses", region_name="us-east-1")
    CHARSET = "UTF-8"
    ses_client.send_email(
        Destination={
            "ToAddresses": [
                email,
            ],
        },
        Message={
            "Body": {
                "Text": {
                    "Charset": CHARSET,
                    "Data": sendMessage,
                }
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": "Dining Suggestions",
            },
        },
        Source="yb2145@nyu.edu",
    )
