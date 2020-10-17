
# The following lambda code will verify the user input towards Lambda
# Also, it will send the customer response to SQS Simple Queue Service

import math
import dateutil.parser
import datetime
import time
import os
import boto3

# --------------- Main handler -----------------------


def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    return dispatch(event)

def dispatch(intent_request):
    intent_name = intent_request['currentIntent']['name']

    # Dispath the intent 
    if intent_name == 'hbl_GreetingIntent':
        return hbl_GreetingIntent(intent_request)
        
    elif intent_name == 'hbl_ThankYouIntent':
        return hbl_ThankyouIntent(intent_request)
        
    elif intent_name == 'hbl_DiningSuggestionIntent':
        return hbl_DiningSuggestionIntent(intent_request)
    else:
        raise Exception('Intent with name ' + intent_name + ' not supported')

# --------------- intents -----------------------

def hbl_GreetingIntent(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Hi there, how can I help you'
        }
    )

def hbl_ThankyouIntent(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Welcome :)'
        }
    )

def hbl_DiningSuggestionIntent(intent_request):
    dining_location = get_slots(intent_request)["location"]
    dining_cuisine = get_slots(intent_request)["cuisine"]
    dining_date = get_slots(intent_request)["date"]
    dining_time = get_slots(intent_request)["time"]
    dining_people = get_slots(intent_request)["people"]
    dining_phone_num = get_slots(intent_request)["number"]

    if intent_request['invocationSource'] == 'DialogCodeHook':
        slots = get_slots(intent_request)
        validation_result = validate_order_restaurants(dining_location, dining_cuisine, dining_date, dining_people, dining_phone_num)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])


        if intent_request[
            'sessionAttributes'] is not None:
            output_session_attributes = intent_request['sessionAttributes']
        else:
            output_session_attributes = {}

        return delegate(output_session_attributes, get_slots(intent_request))
    
    slots = get_slots(intent_request)
    sqs = boto3.client(
        service_name='sqs', 
        region_name='us-east-1'
        )
    
    body = {
        "location":dining_location,
        "cuisine": dining_cuisine,
        "date": dining_date,
        "time": dining_time,
        "people": dining_people,
        "number": dining_phone_num
    }

    response = sqs.send_message(
        QueueUrl = 'https://sqs.us-east-1.amazonaws.com/608484589071/Concierge.fifo',
        MessageBody=str(body)
    )
    
    return close(
        intent_request['sessionAttributes'],
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Your order for a restaurant at {} has been received, We will return with a text at {}'.format(dining_location, dining_phone_num)
            }
        )

def validate_order_restaurants(location, cuisine, date,  people,phone_number ):
    locations_nyc=['new york', 'newyork', 'ny', 'nyc', 'newyorkcity', 'new york city', 'newyork city']
    if location is not None:
        if location.lower() not in locations_nyc:
            return build_validation_result(
                False, 
                'location',
                'For now, you can only choose a restaurant location in New York.'
            )
    
    
    
    cuisine_type = ['indian', 'mexican', 'japanese','chinese', 'coffee']
    if cuisine is not None:
        if cuisine.lower() not in cuisine_type:
            return build_validation_result(
                False,
                'cuisine',
                'For now, you can only choose from indian, mexican, japanese ,chinese, coffee'
            )

        if str(cuisine).isnumeric():
            return build_validation_result(
                False,
                'cuisine',
                'Please enter the cuisine in a correct format.'
                )

    if date:
        if not isvalid_date(date):
            return build_validation_result(
                False, 
                'date', 
                'Please enter a date with a valid format MM-DD-YYYY'
            )
                
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(
                False, 
                'date', 
                'You cannot book for a past time'
            )

    if people is not None:
        if not int(people):
            return build_validation_result(
                False, 
                'people',
                'The input must be an integer'
                )
        if int(people)<1:
            return build_validation_result(
                False, 
                'people',
                'You can only book for a group larger than 1'
            )
        if int(people)>50:
           return build_validation_result(
                False, 
                'people',
                'You can only book for a group less than 50'
           )
    if phone_number is not None:
        phone_number = phone_number.replace('-','')
        if phone_number.startswith('+82') == False:
            return build_validation_result(
                False, 
                'number', 
                'Please enter a phone number that starts with +82'
            )

        elif len(phone_number) != 13:
            return build_validation_result(
                False, 
                'number',
                'The length of the phone number should be 13'
            )

    return build_validation_result(True, None, None)
    

# --------------- Helper function -----------------------
def get_slots(intent_request):
    return intent_request['currentIntent']['slots']
    
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }
def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False



