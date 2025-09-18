from argparse import ArgumentParser
import boto3
import time


parser = ArgumentParser(prog="CreateSpot instance",
                        description="Script for creating spot instances",
                        epilog="Some final message") #TODO: edit epilog

parser.add_argument('--instance_type', '-it', default="c4.xlarge", type=str, required=False)
args = parser.parse_args()

ec2_resource = boto3.resource('ec2')

def create_instance(instance_info):
    max_retry = 5
    for i in range (max_retry):
        try:
            instances = ec2_resource.create_instances(
                IamInstanceProfile=instance_info['IamInstanceProfile'],
                ImageId=instance_info['ImageId'],
                InstanceType=instance_info['InstanceType'],
                KeyName=instance_info['KeyName'],
                MaxCount=instance_info['MaxCount'],
                MinCount=instance_info['MinCount'],
                SecurityGroupIds=instance_info['SecurityGroupIds'],
                InstanceMarketOptions=instance_info['InstanceMarketOptions'],
                TagSpecifications=[
                    {
                        'ResourceType': 'instance',
                        'Tags': [
                            {
                                'Key': "funcx-experiment-date",
                                'Value': "06-08-2024"
                            }
                        ]
                    }
                ],
                Placement=instance_info['Placement'],
                DryRun=False,
            )
            return instances
        except Exception as ex:
            print(f'Cannot create instance with error: {ex}')
            time.sleep(10)
    
    raise Exception(f'Cannot create instance after {max_retry} times, see previous logs for details')


def create_preemptible_instance(instance_type):
    parameters = {
        'IamInstanceProfile': {
            'Arn': 'arn:aws:iam::380285632927:instance-profile/spot-pricing-research-S3-write',
        },
        'ImageId': 'ami-0d630a005b6b0ff3d',
        'InstanceType': instance_type,
        'KeyName': 'masa-openmp-tests2',
        'MaxCount': 1,
        'MinCount': 1,
        'SecurityGroups': [
            'sg-0d1e08208d91533fb'
        ],
        'SecurityGroupIds': [
            'sg-0d1e08208d91533fb'
        ],
        'InstanceMarketOptions':
            {
                'MarketType': 'spot',
                'SpotOptions': {
                    'MaxPrice': "5.00",
                    'SpotInstanceType': 'one-time',
                    'InstanceInterruptionBehavior': 'terminate'
                }
            },
        'Placement': {
            'AvailabilityZone': 'us-east-1a',
        }
    }
    instances = create_instance(parameters)
    created_instances = [i for i in instances]
    return created_instances

created_instances = create_preemptible_instance(args.instance_type)
print(f'Created instance: {created_instances[0]}')