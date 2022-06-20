from ast import While
import boto3

region_name = "cn-northwest-1"
job_name = "sam-data-platform-test"
job_name = "test_workflow"
glue = boto3.client('glue', region_name=region_name)


response = glue.get_job_runs(
    JobName=job_name,
    MaxResults=10
)
count = 0
while 'NextToken' in response:
    runs = response['JobRuns']
    for run in runs:
        status = run['JobRunState']
        if status in ['STARTING', 'RUNNING']:
            print(
                f"{run['Id']}:{run['JobRunState']} begin at {run['StartedOn']}")
            count += 1

    nextToken = response['NextToken']
    response = glue.get_job_runs(
        JobName=job_name,
        MaxResults=10,
        NextToken=nextToken
    )
print(f"============{count}============")
