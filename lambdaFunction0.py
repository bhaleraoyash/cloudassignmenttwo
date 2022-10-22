import json
import boto3

def lambda_handler(event, context):
    client = boto3.client('lex-runtime')
    response = client.post_text(
        botName='DiningConcierge',
        botAlias='DiningAlias',
        userId='NLUKLGFGCL',
        inputText= event['messages'][0]['unstructured']['text']
    )
    message = response['message']
    botResponse = [{
        'type': 'unstructured',
        'unstructured': {
          'text': message
        }
    }]
      
    return {
        'statusCode': 200,
        'messages': botResponse
    }