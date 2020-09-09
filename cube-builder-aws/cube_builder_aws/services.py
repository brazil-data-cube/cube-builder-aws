#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2020 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

import json
import base64
import boto3
import botocore
import requests

from boto3.dynamodb.conditions import Key, Attr
from botocore.errorfactory import ClientError
from stac import STAC

from config import DYNAMO_TB_ACTIVITY, DBNAME_TB_CONTROL, DBNAME_TB_PROCESS, \
    QUEUE_NAME, KINESIS_NAME, LAMBDA_FUNCTION_NAME, \
    AWS_KEY_ID, AWS_SECRET_KEY


class CubeServices:
    
    def __init__(self, url_stac=None, bucket=None):
        # session = boto3.Session(profile_name='default')
        session = boto3.Session(
            aws_access_key_id=AWS_KEY_ID, 
            aws_secret_access_key=AWS_SECRET_KEY)

        # ---------------------------
        # AWS infrastructure
        self.S3client = session.client('s3')
        self.SQSclient = session.client('sqs')
        self.LAMBDAclient = session.client('lambda')
        self.Kinesisclient = session.client('kinesis')
        self.dynamoDBResource = session.resource('dynamodb')

        self.QueueUrl = {}
        self.bucket_name = bucket

        # ---------------------------
        # create / get DynamoDB tables
        self.get_dynamo_tables()

        # ---------------------------
        # create / get the SQS 
        self.get_queue_url()

        # ---------------------------
        # init STAC instance
        self.url_stac = url_stac
        if url_stac:
            self.stac = STAC(url_stac)

    def get_s3_prefix(self, bucket):
        # prefix = 'https://s3.amazonaws.com/{}/'.format(bucket)
        prefix = 's3://{}/'.format(bucket)
        return prefix
    
    ## ----------------------
    # DYNAMO DB
    def get_dynamo_tables(self):
        # Create the cubeBuilderActivities table in DynamoDB to store all activities
        self.activitiesTable = self.dynamoDBResource.Table(DYNAMO_TB_ACTIVITY)
        table_exists = False
        try:
        	self.activitiesTable.creation_date_time
        	table_exists = True
        except:
        	table_exists = False

        if not table_exists:
        	self.activitiesTable = self.dynamoDBResource.create_table(
        		TableName=DYNAMO_TB_ACTIVITY,
        		KeySchema=[
        			{'AttributeName': 'id', 'KeyType': 'HASH' },
        			{'AttributeName': 'sk', 'KeyType': 'RANGE'}
        		],
        		AttributeDefinitions=[
        			{'AttributeName': 'id','AttributeType': 'S'},
        			{'AttributeName': 'sk','AttributeType': 'S'},
        		],
        		BillingMode='PAY_PER_REQUEST',
        	)
            # Wait until the table exists.
        	self.dynamoDBResource.meta.client.get_waiter('table_exists').wait(TableName=DYNAMO_TB_ACTIVITY)

        # Create the cubeBuilderActivitiesControl table in DynamoDB to manage activities completion
        self.activitiesControlTable = self.dynamoDBResource.Table(DBNAME_TB_CONTROL)
        table_exists = False
        try:
        	self.activitiesControlTable.creation_date_time
        	table_exists = True
        except:
        	table_exists = False

        if not table_exists:
        	self.activitiesControlTable = self.dynamoDBResource.create_table(
        		TableName=DBNAME_TB_CONTROL,
        		KeySchema=[
        			{'AttributeName': 'id', 'KeyType': 'HASH' },
        		],
        		AttributeDefinitions=[
        			{'AttributeName': 'id','AttributeType': 'S'},
        		],
        		ProvisionedThroughput={
        			'ReadCapacityUnits': 2,
        			'WriteCapacityUnits': 2
        		}
        	)
            # Wait until the table exists.
        	self.dynamoDBResource.meta.client.get_waiter('table_exists').wait(TableName=DBNAME_TB_CONTROL)

        # Create the cubeBuilderActivitiesControl table in DynamoDB to manage activities completion
        self.processTable = self.dynamoDBResource.Table(DBNAME_TB_PROCESS)
        table_exists = False
        try:
        	self.processTable.creation_date_time
        	table_exists = True
        except:
        	table_exists = False

        if not table_exists:
        	self.processTable = self.dynamoDBResource.create_table(
        		TableName=DBNAME_TB_PROCESS,
        		KeySchema=[
        			{'AttributeName': 'id', 'KeyType': 'HASH' },
        		],
        		AttributeDefinitions=[
        			{'AttributeName': 'id','AttributeType': 'S'},
        		],
        		ProvisionedThroughput={
        			'ReadCapacityUnits': 2,
        			'WriteCapacityUnits': 2
        		}
        	)
            # Wait until the table exists.
        	self.dynamoDBResource.meta.client.get_waiter('table_exists').wait(TableName=DBNAME_TB_PROCESS)
    
    def get_activities(self):
        # self.activitiesTable.meta.client.delete_table(TableName=DYNAMO_TB_ACTIVITY)

        return self.activitiesTable.scan()

    def get_activities_ctrl(self):
        return self.activitiesControlTable.scan()

    def get_activities_by_key(self, dinamo_key):
        return self.activitiesTable.query(
            KeyConditionExpression=Key('id').eq(dinamo_key)
        )

    def get_activity_item(self, query):
        return self.activitiesTable.get_item( 
            Key=query
        )

    def get_process_by_id(self, process_id):
        return self.processTable.query(
            KeyConditionExpression=Key('id').eq(process_id)
        )

    def get_process_by_datacube(self, datacube):
        return self.processTable.scan(
            FilterExpression=Key('datacube').eq(datacube)
        )

    def get_cube_meta(self, cube,):
        filters = Key('data_cube').eq(cube) & Key('id').begins_with('merge')

        return self.activitiesTable.scan(
            FilterExpression=filters,
        )

    def get_all_items(self, filters):
        response = self.activitiesTable.scan(
            FilterExpression=filters,
            Limit=100000000
        )

        items = response['Items']

        while 'LastEvaluatedKey' in response:
            response = self.activitiesTable.scan(
                FilterExpression=filters,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response['Items'])

        return items

    def get_merges(self, data_cube: str, tile_id: str, start: str, end: str):
        """List all merges activities used to build a data cube.

        Args:
            data_cube - Data cube name
            start - Filter start data
            end - Filter end data
        """
        expression = Key('tile_id').eq(tile_id) & Key('period_start').between(start, end) & \
            Key('period_end').between(start, end) & Key('data_cube').eq(data_cube)

        return self.get_all_items(expression)

    def get_activities_by_datacube(self, data_cube: str):
        """List all activities used to build a data cube.

        Args:
            data_cube - Data cube name
        """
        expression = Key('data_cube').eq(data_cube)
        return self.get_all_items(expression)

    def put_activity(self, activity):
        self.activitiesTable.put_item(
            Item={
                'id': activity['dynamoKey'],
                'sk': activity['sk'],

                'tile_id': activity['tileid'],
                'period_start': activity['start'],
                'period_end': activity['end'],
                'data_cube': activity['datacube'],

                'mystatus': activity['mystatus'],
                'mylaunch': activity['mylaunch'],
                'mystart': activity['mystart'],
                'myend': activity['myend'],
                'efficacy': activity['efficacy'],
                'cloudratio': activity['cloudratio'],
                'instancesToBeDone': activity['instancesToBeDone'],
                'totalInstancesToBeDone': activity['totalInstancesToBeDone'],
                'activity': json.dumps(activity),
            }
        )
        return True

    def put_process_table(self, key, infos):
        self.processTable.put_item(
            Item = {
                'id': key,
                'datacube': infos['datacube'],
                'datacube_suffix': infos['datacube_suffix'],
                'functions': json.dumps(infos['composite_function']),
                'indexes': json.dumps(infos['indexes']),
                'quality_band': infos['quality_band'],
                'platform_code': infos['platform_code'],
            }
        )
        return True

    def put_control_table(self, key, value):
        self.activitiesControlTable.put_item(
            Item = {
                'id': key,
                'mycount': value,
            }
        )
        return True

    def remove_control_by_key(self, key: str):
        try:
            self.activitiesControlTable.delete_item(
                Key=dict(id=key)
            )
            return True
        except:
            return False

    def remove_process_by_key(self, key: str):
        try:
            self.processTable.delete_item(
                Key=dict(id=key)
            )
            return True
        except:
            return False

    def update_control_table(self, Key, UpdateExpression, ExpressionAttributeNames, ExpressionAttributeValues, ReturnValues):
        return self.activitiesControlTable.update_item(
            Key=Key,
            UpdateExpression=UpdateExpression,
            ExpressionAttributeNames=ExpressionAttributeNames,
            ExpressionAttributeValues=ExpressionAttributeValues,
            ReturnValues=ReturnValues
        )

    ## ----------------------
    # SQS
    def get_queue_url(self):
        for action in ['merge', 'blend', 'posblend', 'publish']:
            queue = '{}-{}'.format(QUEUE_NAME, action)
            if self.QueueUrl.get(action, None) is not None:
                continue
            response = self.SQSclient.list_queues()
            q_exists = False
            if 'QueueUrls' in response:
                for qurl in response['QueueUrls']:
                    if qurl.find(queue) != -1:
                        q_exists = True
                        self.QueueUrl[action] = qurl
            if not q_exists:
                self.create_queue(True, action)
        return True

    def create_queue(self, create_mapping = False, action = ''):
        """
        As the influx of messages to a queue increases, AWS Lambda automatically scales up 
        polling activity until the number of concurrent function executions reaches 1000, 
        the account concurrency limit, or the (optional) function concurrency limit, 
        whichever is lower. 
        Amazon Simple Queue Service supports an initial burst of 5 concurrent function invocations 
        and increases concurrency by 60 concurrent invocations per minute.

        So, for example, 
        1000 messages arrives at the queue at once, only 5 will be processed in the first minute.
        65 lambdas will run concurrently in the second minute... so on
		"""
        # Create a SQS for this experiment
        queue = '{}-{}'.format(QUEUE_NAME, action)
        response = self.SQSclient.create_queue(
            QueueName=queue,
            Attributes={'VisibilityTimeout': '500'}
        )
        self.QueueUrl[action] = response['QueueUrl']
        # Get attributes
        attributes = self.SQSclient.get_queue_attributes(QueueUrl=self.QueueUrl[action], AttributeNames=['All',])
        QueueArn = attributes['Attributes']['QueueArn']

        # Create Source Mapping to Maestro from queue
        if create_mapping:
            response = self.LAMBDAclient.create_event_source_mapping(
                EventSourceArn=QueueArn,
                FunctionName=LAMBDA_FUNCTION_NAME,
                Enabled=True,
                BatchSize=1
            )

    def send_to_sqs(self, activity):
        if self.get_queue_url():
            action = activity['action']
            self.SQSclient.send_message(QueueUrl=self.QueueUrl[action], MessageBody=json.dumps(activity))

    
    ## ----------------------
    # Kinesis
    def put_item_kinesis(self, activity):
        activity['channel'] = 'kinesis'
        activity['db'] = 'dynamodb'
        status = self.sendToKinesis(activity)
        del activity['channel']
        del activity['db']
        return status

    def sendToKinesis(self, activity):
        self.Kinesisclient.put_record(
			StreamName=KINESIS_NAME,
			Data=json.dumps(activity),
    		PartitionKey='dsKinesis'
		)
        return True


    ## ----------------------
    # STAC
    def get_collection_stac(self, collection_id):
        _ = self.stac.catalog
        return self.stac.collection(collection_id)

    def search_STAC(self, activity):
        # Get DATACUBE params
        _ = self.stac.catalog
        bands = activity['bands']
        datasets = activity['datasets']
        bbox = activity['bbox']
        time = '{}/{}'.format(activity['start'], activity['end'])

        scenes = {}
        for dataset in datasets:
            filter_opts = dict(
                time=time,
                bbox=bbox,
                limit=10000
            )
            items = self.stac.collection(dataset).get_items(filter=filter_opts)

            for f in items['features']:
                if f['type'] == 'Feature':
                    id = f['id']
                    date = f['properties']['datetime'] 
                    
                    # Get file link and name
                    assets = f['assets']
                    for band in bands:
                        band_obj = assets.get(band, None)
                        if not band_obj: continue

                        scenes[band] = scenes.get(band, {})
                        scenes[band][dataset] = scenes[band].get(dataset, {})

                        scene = {}
                        scene['sceneid'] = id
                        scene['date'] = date
                        scene['band'] = band
                        scene['link'] = band_obj['href']
                        if dataset == 'MOD13Q1' and band == activity['quality_band']:
                            scene['link'] = scene['link'].replace(activity['quality_band'],'reliability')

                        # TODO: verify if scene['link'] exist 

                        if date not in scenes[band][dataset]:
                            scenes[band][dataset][date] = []
                        scenes[band][dataset][date].append(scene)
        return scenes
    
    ## ----------------------
    # S3
    def create_bucket(self, name, requester_pay=True):
        try:
            # Create a bucket with public access
            response = self.S3client.create_bucket(
                ACL='public-read',
                Bucket=name
            )
            if requester_pay:
                response = self.S3client.put_bucket_request_payment(
                    Bucket=name,
                    RequestPaymentConfiguration={
                        'Payer': 'Requester'
                    }
                )
            assert response['ResponseMetadata']['HTTPStatusCode'] == 200
            return True
        except ClientError:
            return False
        return True

    def s3_file_exists(self, bucket_name=None, key=''):
        try:
            if not bucket_name:
                bucket_name = self.bucket_name
            return self.S3client.head_object(Bucket=bucket_name, Key=key)
        except ClientError:
            return False

    def delete_file_S3(self, bucket_name=None, key=''):
        try:
            if not bucket_name:
                bucket_name = self.bucket_name
            self.S3client.delete_object(Bucket=bucket_name, Key=key)
        except ClientError:
            return False
        return True

    def save_file_S3(self, bucket_name=None, key='', activity={}):
        if not bucket_name:
            bucket_name = self.bucket_name
        return self.S3client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=(bytes(json.dumps(activity).encode('UTF-8')))
        )

    def upload_file_S3(self, memfile, key, args, bucket_name=None):
        if not bucket_name:
            bucket_name = self.bucket_name
        return self.S3client.upload_file(
            memfile, 
            Bucket=bucket_name,
            Key=key,
            ExtraArgs=args
        )

    def upload_fileobj_S3(self, memfile, key, args, bucket_name=None):
        if not bucket_name:
            bucket_name = self.bucket_name
        return self.S3client.upload_fileobj(
            memfile, 
            Bucket=bucket_name,
            Key=key,
            ExtraArgs=args
        )

    def list_repositories(self):
        return [bucket['Name'] for bucket in self.S3client.list_buckets()['Buckets']]