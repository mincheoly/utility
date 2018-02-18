"""
	Tools to use in IO. 

	Mostly dealing with Amazon infrastructure (S3).
"""


import pickle as pkl
import boto3
import numpy as np
import pandas as pd
import StringIO


def get_file(path):
	""" Get a general file as a byte array. """

	parts = path.split('/')

	bucket = parts[2]

	key = '/'.join(parts[3:])

	s3_client = boto3.client('s3')

	response = s3_client.get_object(Bucket=bucket,Key=key)

	return StringIO.StringIO(response['Body'].read())


def write_df(df, path, sep=',', encoding='ascii'):
	""" Write a Pandas DataFrame as a csv file in S3. """

	parts = path.split('/')

	bucket = parts[2]

	key = '/'.join(parts[3:])

	text = df.to_csv(index=False, sep=sep, encoding=encoding)

	s3_resource = boto3.resource('s3')

	response = s3_resource.Object(bucket, key).put(Body=text)
	
	return response