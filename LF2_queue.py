import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import ast
from random import randint
import requests

# boto3 = boto3.Session(profile_name = 'nyu')
QUEUE_URL= 'https://queue.amazonaws.com/608484589071/Concierge'

def dequeue():
    sqs= boto3.client('sqs', region_name='us-east-1')
    
    try:
        sqs_response = sqs.receive_message(
        QueueUrl = QUEUE_URL,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout = 0,
        WaitTimeSeconds = 0
        )
        # Retrieve message from SQS
        message = sqs_response['Messages'][0]['Body']
        json_message = ast.literal_eval(message)
        res_location = json_message['location']
        res_cuisine = json_message['cuisine']
        res_date = json_message['date']
        res_time = json_message['time']
        res_people = json_message['people']
        res_number = json_message['number']
        
        #After retrieving, delete message
        receipt_handle = sqs_response['Messages'][0]['ReceiptHandle']
        sqs.delete_message(
            QueueUrl = QUEUE_URL,
            ReceiptHandle = receipt_handle
        )

    except:
        print('Error while retrieving message from SQS!')

    return (res_location, res_cuisine, res_date, res_time, res_people, res_number)

def rand_elastic_search(location, cuisine):
    # Pick a random business ID from elastic search with the retrieved cuisine
    r = requests.get('https://search-yelpyelprestaurant-ujbkvdusitv4oijcy4okrpppgm.us-east-2.es.amazonaws.com/restaurants/_search?q='+str(cuisine))
    rand_data = r.json()
    value = randint(1,5)
    try:
        rand_buss_id = rand_data["hits"]["hits"][value]['_source']['Business_id']
        return rand_buss_id
    except IndexError:
        return (f"There's no data for {cuisine} in {location}")
# Using the business ID retrieve information about the restaurant from dynamo db

def dynamodb_search(rand_business_id):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table_db = dynamodb.Table("YelpRestaurant")
    scanresult  = table_db.scan(FilterExpression=Attr('Business_id').eq(rand_business_id))
    item = scanresult['Items']
    
    name = item[0]['Name']
    address = item[0]['Address']
    review_count = item[0]['NumberOfReviews']
    rating = item[0]['Rating']
    print(item)
    message = f"Our recommendation is {name} in {address}. It has {review_count} reviews and a rating of {rating}!!! :)"
    return message
    # Now push a message to the customer using 'SNS"

def sendsns(message, number):
    sns = boto3.client('sns', region_name='us-east-1')
    response = sns.publish(
        PhoneNumber = number,
        Message = message,
        MessageStructure = 'string'
    )
   
    print(response)
    return(response)

def lambda_handler(event, context):
    location, cuisine, date, time, people, number  = dequeue()
    print(location, cuisine, date, time, people, number)
    rand_business_id = rand_elastic_search(location, cuisine)
    if rand_business_id.startswith("There's no"):
        print(rand_business_id)
    else:
        message = dynamodb_search(rand_business_id)
        if message.startswith("Our recommendation is"):
            sendsns(message, number)
        else:
            ("Cannot send message")
