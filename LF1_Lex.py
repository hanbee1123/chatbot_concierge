# The following lambda code will verify the user input towards Lambda
# Also, it will send the customer response to SQS Simple Queue Service

import math
import dateutil
import datetime
import time
import os

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
        
    elif intent_name == 'hbl_ThankyouIntent':
        return hbl_ThankyouIntent(intent_request)
        
    elif intent_name == 'hbl_DiningSuggestionIntent':
        return hbl_DiningSuggestionIntent(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')

# --------------- intents -----------------------

def hbl_GreetingIntent(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Hi there, which location are you looking to dine out???'
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
    dining_date = get_slots(intent_request)["date"]
    dining_time = get_slots(intent_request)["time"]
    dining_people = get_slots(intent_request)["people"]
    dining_phone_num = get_slots(intent_request)["number"]

    # Validate date
        #Check format of the date first
    if valid_date(dining_date) == False:
        return build_validation_result(
            False,
            'date',
            'Please enter date in the following format : MM-DD-YYYY'
        )
    
        # Check if date isn't in the past
    date_requested = dateutil.parser.parse(dining_date)
    past_date = datetime.datetime.today() - datetime.timedelta(days=1)
    if date_requested < past_date:
        return build_validation_result(
            False,
            'date',
            'You cannot make reservation for past date'
        )

    # Validate time
    if dining_time:
        time_requested = dateutil.parser.parse(dining_time)
        if time_requested < datetime.datetime.now():
            return build_validation_result(
                False,
                'time',
                'You cannot make reservation for past time!'
            )

    # Validate number of people
    if dining_people.isdigit() == False:
        return build_validation_result(
            False,
            'people',
            'Please enter a valid integer'
        )
    
    if (0<int(dining_people)<50) == False:
        return build_validation_result(
            False,
            'people',
            'You can only make reservation for up to 50 people'
        )
    
    # Validate phone_number
    if phone_number.isdigit() == False:
        return build_validation_result(
            False,
            'number',
            'Please enter a phone number in the following format XXXXXXXXXX'
        )

    if (10<=len(phone_numer)<=11) == False:
        return build_validation_result(
            False,
            'number',
            'Please enter a phone number that is 10 or 11 digits long'
        )




# --------------- Helper function -----------------------
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

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

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

def valid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False