import boto3

sqs = boto3.resource('sqs')

# Create the queue. This returns an SQS.Queue instance
queue = sqs.create_queue(QueueName='queue-name')

# You can now access identifiers and attributes
print(queue.url)