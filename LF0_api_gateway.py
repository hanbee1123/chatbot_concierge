# LF0 will take the message from client through the API gateway then, post it to LEX

import json
import boto3

client = boto3.client('lex-runtime')

def lambda_handler(event, context):
    user_message = event['message']
    if user_message == None:
        return{
            'statusCode': 200,
            'body':json.dumps("Error occured!")
        }

    response = client.post_text(
        botName='chatbot_concierge',
        botAlias='concierge',
        userId= admin,
        inputText= message
    )

    print(response)
    return {
        'statusCode': 200,
        'body': response
    }