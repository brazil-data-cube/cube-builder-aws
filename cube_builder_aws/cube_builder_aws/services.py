#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2021 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

import json
import re
from urllib.parse import urlparse

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.errorfactory import ClientError
from stac import STAC

from .config import (AWS_KEY_ID, AWS_SECRET_KEY, DBNAME_TB_CONTROL,
                     DBNAME_TB_HARM, DBNAME_TB_PROCESS, DYNAMO_TB_ACTIVITY,
                     KINESIS_NAME, LAMBDA_FUNCTION_NAME, QUEUE_NAME)


class CubeServices:
    
    def __init__(self, bucket=None, stac_list=[]):
        # session = boto3.Session(profile_name='default')
        self.session = session = boto3.Session(
            aws_access_key_id=AWS_KEY_ID, 
            aws_secret_access_key=AWS_SECRET_KEY)

        # ---------------------------
        # AWS infrastructure
        self.S3client = session.client('s3')
        self.SQSclient = session.client('sqs')
        self.LAMBDAclient = session.client('lambda')
        self.Kinesisclient = session.client('kinesis')
        self.dynamoDBResource = session.resource('dynamodb')

        self.bucket_name = bucket

        # ---------------------------
        # create / get DynamoDB tables
        self.get_dynamo_tables()

        # ---------------------------
        # create / get the SQS 
        self.QueueUrl = {}
        self.get_queue_url()

        # ---------------------------
        # init STAC instance
        self.stac_list = []
        for stac in stac_list:
            stac_instance = STAC(stac["url"], access_token=stac["token"]) if stac.get("token", None) else STAC(stac["url"])
            self.stac_list.append(
                dict(**stac, instance=stac_instance)
            )

    def get_s3_prefix(self, bucket):
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
        		BillingMode='PAY_PER_REQUEST',
        	)
            # Wait until the table exists.
        	self.dynamoDBResource.meta.client.get_waiter('table_exists').wait(TableName=DBNAME_TB_CONTROL)

        # Create the cubeBuilderProcess table in DynamoDB to manage process/cubes started
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


        # Create the cubeBuilderActivitiesHarmonization table in DynamoDB to manage activities completion
        self.harmTable = self.dynamoDBResource.Table(DBNAME_TB_HARM)
        table_exists = False
        try:
        	self.harmTable.creation_date_time
        	table_exists = True
        except:
        	table_exists = False

        if not table_exists:
        	self.harmTable = self.dynamoDBResource.create_table(
        		TableName=DBNAME_TB_HARM,
        		KeySchema=[
        			{'AttributeName': 'id', 'KeyType': 'HASH' },
        		],
        		AttributeDefinitions=[
        			{'AttributeName': 'id','AttributeType': 'S'},
        		],
        		BillingMode='PAY_PER_REQUEST',
        	)
            # Wait until the table exists.
        	self.dynamoDBResource.meta.client.get_waiter('table_exists').wait(TableName=DBNAME_TB_HARM)

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

    def get_process_by_datacube(self, datacube_id):
        return self.processTable.scan(
            FilterExpression=Key('datacube_id').eq(datacube_id)
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
                'data_cube': activity['datacube'] if activity['action'] != 'merge' else activity['irregular_datacube'],

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


    def put_harmonization_activity(self, activity):
        self.harmTable.put_item(
            Item={
                'id': activity['dynamoKey'],
                'mystatus': activity['mystatus'],
                'mystart': activity['mystart'],
                'myend': activity['myend'],
                'activity': json.dumps(activity),
            }
        )
        return True

    def put_process_table(self, key, datacube_id, i_datacube_id, infos):
        self.processTable.put_item(
            Item = {
                'id': key,
                'datacube_id': datacube_id,
                'irregular_datacube_id': i_datacube_id,
                'infos': infos
            }
        )
        return True

    def put_control_table(self, key, value, value_total, date):
        self.activitiesControlTable.put_item(
            Item = {
                'id': key,
                'mycount': value,
                'tobe_done': value_total,
                'start_date': date,
                'end_date': date,
                'errors': 0
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

    def remove_activity_by_key(self, key: str, sk: str):
        try:
            self.activitiesTable.delete_item(
                Key=dict(id=key, sk=sk)
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

    def get_control_activities(self, data_cube):
        expression = Attr('id').contains(data_cube)
        response = self.activitiesControlTable.scan(
            FilterExpression=expression,
            Limit=1000000
        )

        items = response['Items']

        while 'LastEvaluatedKey' in response:
            response = self.activitiesControlTable.scan(
                FilterExpression=expression,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response['Items'])

        return items

    ## ----------------------
    # SQS
    def get_queue_url(self):
        for action in ['harmonization', 'merge', 'blend', 'posblend', 'publish']:
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

    def _parse_stac_result(self, items, dataset, bands, harm_bands_map):
        scenes = dict()

        for f in items['features']:

            if f['type'] == 'Feature':
                id = f['id']
                date = f['properties']['datetime'][:10]
                platform = f['properties'].get('platform', None)

                # Get file link and name
                assets = f['assets']
                for band in bands:
                    band_name = band
                    if platform and harm_bands_map.get(platform):
                        if harm_bands_map[platform].get(band):
                            band_name = harm_bands_map[platform][band]
                        else:
                            continue

                    band_obj = assets.get(band_name, None) or assets.get(f'{band_name}.TIF', None)
                    if not band_obj: 
                        continue

                    scenes[band] = scenes.get(band, {})
                    scenes[band][dataset] = scenes[band].get(dataset, {})

                    scene = {}
                    scene['sceneid'] = id
                    scene['date'] = date
                    scene['band'] = band
                    scene['original_band_name'] = band_name
                    scene['platform'] = platform
                    scene['link'] = band_obj['href']

                    if f['properties'].get('eo:bands'):
                        for _band in f['properties']['eo:bands']:
                            if _band['name'] == band_name and 'nodata' in _band:
                                scene['source_nodata'] = _band['nodata']
                                break

                    if scene['link'].startswith('https://landsatlook.usgs.gov/data/'):
                        scene['link'] = scene['link'].replace('https://landsatlook.usgs.gov/data/', 's3://usgs-landsat/')

                    if re.match(r'https://([a-zA-Z0-9-_]{1,}).s3.([a-zA-Z0-9-_]{1,}).amazonaws.com/([-.a-zA-Z0-9\/_]{1,})', scene['link']):
                        parser = urlparse(scene['link'])
                        scene['link'] = f's3://{parser.hostname.split(".")[0]}{parser.path}'

                    # TODO: verify if scene['link'] exist

                    if date not in scenes[band][dataset]:
                        scenes[band][dataset][date] = []
                    scenes[band][dataset][date].append(scene)

        return scenes

    def search_STAC(self, activity):
        """Search for activity in remote STAC server.

        Notes:
            By default, uses entire activity to search for data catalog.
            When the parameter ``extra_catalog`` is set, this function will seek
            into given catalogs and then merge the result as a single query server.
            It may be useful if you have a collection in different server provider.

        Args:
            activity (dict): Current activity scope with default STAC server and stac collection
                **Make sure that ``geom`` property is a GeoJSON Feature.
            extra_catalogs (List[dict]): Extra catalogs to seek for collection. Default is None.
        """
        # Get DATACUBE params
        bands = activity['bands']
        bbox_feature = activity['geom']
        time = '{}/{}'.format(activity['start'], activity['end'])

        scenes = {}
        collection_ref = ''
        filter_opts = dict(
            datetime=time,
            time=time,
            intersects=bbox_feature,
            limit=10000
        )

        harm_bands_map = dict()
        if activity.get('landsat_harmonization') and activity['landsat_harmonization'].get('map_bands'):
            harm_bands_map = activity['landsat_harmonization']['map_bands']

        for stac in self.stac_list:
            _ = stac['instance'].catalog

            filter_opts['collections'] = [stac['collection']]
            # TODO: STAC https://landsatlook.usgs.gov/  does not conform to specification
            if '.usgs.gov' in stac['url']:
                filter_opts['query'] = dict(collections=[stac['collection']])
            items = stac['instance'].search(filter=filter_opts)

            res = self._parse_stac_result(items, stac['collection'], bands, harm_bands_map)

            if not scenes:
                scenes.update(**res)
                collection_ref = stac['collection']
            else:
                for band, datasets in res.items():
                    dataset = list(datasets.keys())[0]

                    for date in datasets[dataset]:
                        # if date in scenes : sum items in date list
                        if date in scenes[band][collection_ref]:
                            for item in datasets[dataset][date]:
                                scenes[band][collection_ref][date].append(item)

                        else:
                            scenes[band][collection_ref][date] = datasets[dataset][date]

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

    def s3_file_exists(self, bucket_name=None, key='', request_payer=False):
        try:
            if not bucket_name:
                bucket_name = self.bucket_name
            if request_payer:
                return self.S3client.head_object(Bucket=bucket_name, Key=key, RequestPayer='requester')
            else:
                return self.S3client.head_object(Bucket=bucket_name, Key=key)
        except ClientError:
            return False

    def get_object(self, key, bucket_name=None):
        return self.S3client.get_object(Bucket=bucket_name, Key=key)

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