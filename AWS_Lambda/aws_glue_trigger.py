import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):

	#get s3 creds from event handler
	obj_key = event['detail']['object']['key']

	#define glue job and args
	glue_job = 'transform_jog'
	job_args = {
		'--obj_key': obj_key,
		'--s3sourcepath': 's3path',
	}

	#intialize glue job
	glue = boto3.client('glue')
	print(f'starting glue job: {glue_job}')
	#start glue job
	try:
		response = glue.start_job_run(JobName=glue_job, Arguments=job_args)
		return response
	except ClientError as e:
		raise Exception("boto3 client error: " + e.__str__())
	except Exception as e:
		raise Exception("Unexepected error:" + e.__str__())