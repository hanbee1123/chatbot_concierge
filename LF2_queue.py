import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import ast
from random import randint
import requests

# --------------- Constant Variable -----------------------
# boto3 = boto3.Session(profile_name = 'nyu')
QUEUE_URL= 'https://queue.amazonaws.com/608484589071/Concierge'



# ----------- function to dequeue message from SQS ------------------

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

    # If no message, print error without return value
    except IndexError:
        return ('Error while retrieving message from SQS!')

    # Return values
    return (res_location, res_cuisine, res_date, res_time, res_people, res_number)



# ----------- Perform elastic search with cuisine keyword from SQS message ------------------

def rand_elastic_search(location, cuisine):
    # Collect restaurant data from elastic search
    r = requests.get('https://search-yelpyelprestaurant-ujbkvdusitv4oijcy4okrpppgm.us-east-2.es.amazonaws.com/restaurants/_search?q='+str(cuisine))
    rand_data = r.json()
    # Choose random among returned list
    value = randint(1,5)
    try:
        rand_buss_id = rand_data["hits"]["hits"][value]['_source']['Business_id']
        return rand_buss_id
    except IndexError:
        return (f"There's no data for {cuisine} in {location}")

# ----------- Search DynamoDB to find more information about chosen restaurant ------------------

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


# ----------- Send message with information about the restaurant ------------------

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
    # Collect message from SQS
    location, cuisine, date, time, people, number  = dequeue()
    print(location, cuisine, date, time, people, number)

    # Choose a random restaurant with the given cuisine
    rand_business_id = rand_elastic_search(location, cuisine)
    if rand_business_id.startswith("There's no"):
        print(rand_business_id)
    else:

        # Find detailed information about the restaurant
        message = dynamodb_search(rand_business_id)

        # Send SNS message
        if message.startswith("Our recommendation is"):
            sendsns(message, number)
        else:
            print ("Cannot send message")
