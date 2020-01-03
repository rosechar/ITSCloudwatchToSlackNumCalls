import boto3
import json
import logging
import os
from datetime import datetime
from datetime import timedelta

from base64 import b64decode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError



# The Slack channel to send a message to stored in the slackChannel environment variable
SLACK_CHANNEL = os.environ['slackChannel']

HOOK_URL = 'https://hooks.slack.com/services/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'


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    client = boto3.client('cloudwatch', region_name = 'us-east-1')
    currentTime = datetime.now()
    input = json.loads(event['Records'][0]['Sns']['Message'])
    instanceID = input['Trigger']['Dimensions'][0]['value']
    queueName = input['Trigger']['Dimensions'][2]['value']
    metricData = client.get_metric_data(
        MetricDataQueries=[
            {
            'Id': 'numberOfCalls',
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/Connect',
                    'MetricName': 'QueueSize',
                    'Dimensions': [
                        {
                            'Name': 'InstanceId', 
                            'Value': instanceID
                        }, 
                        {
                            'Name': 'MetricGroup', 
                            'Value': 'Queue'
                        }, 
                        {
                            'Name': 'QueueName', 
                            'Value': queueName
                        },
                    ]
                },
                'Period': 300,
                'Stat': 'Maximum',
                'Unit': 'Count'
            },
            'Label': 'Retrieves the number of MiWorkspace calls in queue',
            'ReturnData': True
        },
    ],
    # datetime(year, month, day, hour, minute, second, microsecond)
    StartTime=(datetime.now() - timedelta(minutes=10)),
    EndTime=datetime.now(),
    ScanBy='TimestampDescending'
    )
    queueSize = int(metricData['MetricDataResults'][0]['Values'][0])
    slack_message = {
        'channel': SLACK_CHANNEL,
        'text': 'There are currently ' + str(queueSize) + ' calls waiting in the ' + queueName + ' queue'
    }
    
    req = Request(HOOK_URL, json.dumps(slack_message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        logger.info('Message posted to %s', slack_message['channel'])
    except HTTPError as e:
        logger.error('Request failed: %d %s', e.code, e.reason)
    except URLError as e:
        logger.error('Server connection failed: %s', e.reason)
    