import json
import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def lambda_handler(event, context):
  os.environ['TZ'] = 'America/New_York'
  time.tzset()
  logger.debug('event.bot.name={}'.format(event['bot']['name']))
  return dispatch(event)


def dispatch(intent_request):
  logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))
  intent_name = intent_request['currentIntent']['name']
  if intent_name == 'DiningSuggestionsIntent':
      return give_recommendations(intent_request)
  raise Exception('Intent with name ' + intent_name + ' not supported')

def get_slots(intent_request):
  return intent_request['currentIntent']['slots']

def give_recommendations(intent_request):
  location_type = get_slots(intent_request)["slotOne"]
  cuisine_type = get_slots(intent_request)["slotTwo"]
  dining_time = get_slots(intent_request)["slotThree"]
  quantity = get_slots(intent_request)["slotFour"]
  email = get_slots(intent_request)["slotFive"]
  source = intent_request['invocationSource']

  if source == 'DialogCodeHook':
      slots = get_slots(intent_request)
      validation_result = validate_order_flowers(location_type, cuisine_type, dining_time)
      if not validation_result['isValid']:
          slots[validation_result['violatedSlot']] = None
          return elicit_slot(intent_request['sessionAttributes'],
                             intent_request['currentIntent']['name'],
                             slots,
                             validation_result['violatedSlot'],
                             validation_result['message'])

      # Pass the price of the flowers back through session attributes to be used in various prompts defined
      # on the bot model.
      output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
      if location_type is not None:
          output_session_attributes['Location'] = location_type  # Elegant pricing model

      return delegate(output_session_attributes, get_slots(intent_request))

  messageBody = {"Location": location_type, "Cuisine": cuisine_type, "Dining Time": dining_time, "Number of people": quantity, "Email": email}
  
  messageBody = json.dumps(messageBody)

  sqs = boto3.client('sqs')
  sqs.send_message(
    QueueUrl="https://sqs.us-east-1.amazonaws.com/283227796002/queue-name",
    MessageBody=messageBody
  )
      

  # Order the flowers, and rely on the goodbye message of the bot to define the message to the end user.
  # In a real bot, this would likely involve a call to a backend service.
  return close(intent_request['sessionAttributes'],
               'Fulfilled',
               {'contentType': 'PlainText',
                'content': 'Thanks, your request has been placed, we will notify you via email when we have the recommendations ready.'})

def validate_order_flowers(location_type, cuisine_type, dining_time):
    location_types = ['manhattan', 'bronx', 'brooklyn', 'queens', 'staten island']
    if location_type is not None and location_type.lower() not in location_types:
        return build_validation_result(False,
                                       'slotOne',
                                       'We do not have {}, would you like a different type of location?  '
                                       'Our most popular location is Manhattan'.format(location_type))

    cuisine_types = ['indian', 'chinese', 'mexican']
    if cuisine_type is not None and cuisine_type.lower() not in cuisine_types:
        return build_validation_result(False,
                                       'slotTwo',
                                       'We do not have {}, would you like a different type of cuisine?  '
                                       'Our most popular cuisine is Indian'.format(cuisine_type))

    if dining_time is not None:
        if len(dining_time) != 5:
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'slotTwo', None)

        hour, minute = dining_time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'slotTwo', None)

        if hour < 10 or hour > 16:
            # Outside of business hours
            return build_validation_result(False, 'slotTwo', 'Our business hours are from ten a m. to five p m. Can you specify a time during this range?')

    return build_validation_result(True, None, None)


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

def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


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