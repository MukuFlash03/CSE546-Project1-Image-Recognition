import base64
from io import BytesIO
import logging

import torch
import torchvision
import torchvision.transforms as transforms
import torch.nn as nn
import torch.nn.functional as F
import torchvision.models as models
from urllib.request import urlopen
from PIL import Image
import numpy as np
import json
import sys
import time
import boto3
import uuid
import time

# define client for sqs and s3
sqs = boto3.client('sqs',region_name="us-east-1")
queue_url = 'https://sqs.us-east-1.amazonaws.com/874290406143/request_queue'
response_queue_url = 'https://sqs.us-east-1.amazonaws.com/874290406143/response-queue'

output_bucket_name = 'output-bucket-results231'
s3 = boto3.client('s3', region_name='us-east-1')

# define file handler for capturing logs
file_handler = logging.FileHandler('/home/ubuntu/classification.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
file_handler.setFormatter(formatter)
logging.basicConfig(filename='/home/ubuntu/classification.log', level=logging.DEBUG)

try:
    while True:
        #long polling for messages in request queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )
        if 'Messages' in response:
            message = response['Messages'][0]
            logging.debug('Received message from SQS', message)
            message_body = json.loads(message['Body'])
            image_data = message_body['image']

            # Logging ID for tracing
            logging.debug(f"Processing message Id: {message_body['id']}")

            # Decode the base64-encoded image data
            decoded_data = base64.b64decode(image_data)

            # Load the decoded data into a PIL image
            img = Image.open(BytesIO(decoded_data))
            model = models.resnet18(pretrained=True)

            model.eval()
            img_tensor = transforms.ToTensor()(img).unsqueeze_(0)
            outputs = model(img_tensor)
            _, predicted = torch.max(outputs.data, 1)

            with open('/home/ubuntu/imagenet-labels.json') as f:
                labels = json.load(f)
            result = labels[np.array(predicted)[0]]
            img_name = "received_image"
            logging.info("Classification result of {} is {}".format(message_body['id'],result))
            img_values = [message_body['id'],result]
            bucket_value = ','.join(img_values)
            # storing output in output bucket
            s3.put_object(
                Bucket=output_bucket_name,
                Key=message_body['id'],
                Body=bucket_value
            )
            logging.info("Image sent to Output Bucket successfully.")
            # Send classified image results to SQS Response Queue
            message_response = {
                'id':message_body['id'],
                'results':result
            }
            response = sqs.send_message(
                QueueUrl= response_queue_url,
                MessageBody= json.dumps(message_response)
            )

            # Delete the message from the queue
            logging.info("Deleting message from request queue")
            receipt_handle = message['ReceiptHandle']
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            print("Successfully processed id {}".format(message_body['id']))

            # Introduce a delay
            logging.info("Sleeping for 10 sec")
            time.sleep(10)
            logging.info("Awake!")
        else:
            logging.info("No messages in queue")
except Exception as exp:
    logging.error(exp)

