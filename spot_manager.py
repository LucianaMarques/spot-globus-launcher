import boto3
import time


from base_class import BaseClass


class SpotManager(BaseClass):
    def __init__(self, pool_num):
        BaseClass.__init__(self, pool_num)
        self.configure_logging()
        self.interruption_behaviour = "terminate"
        self.ec2_client = boto3.client('ec2')
        self.ec2_resource = boto3.resource('ec2')
        self.max_retry = 5


    def create_instance(self, instance_info):
        for i in range (self.max_retry):
            self.log_info(f'Attempt {i+1} of creating instance')
            try:
                instances = self.ec2_resource.create_instances(
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
                self.log_info(f'Cannot create instance with error: {ex}')
                time.sleep(10)
        
        raise Exception(f'Cannot create instance after {self.max_retry} times, see previous logs for details')


    def create_preemptible_instance(self, instance_type):
        parameters = {
            'IamInstanceProfile': {
                'Arn': self.config.iam_arn,
            },
            'ImageId': self.config.ami_id,
            'InstanceType': instance_type,
            'KeyName': self.config.rsa_file_name,
            'MaxCount': 1,
            'MinCount': 1,
            'SecurityGroups': [
                self.config.rg_name
            ],
            'SecurityGroupIds': [
                self.config.rg_name
            ],
            'InstanceMarketOptions':
                {
                    'MarketType': 'spot',
                    'SpotOptions': {
                        'MaxPrice': "5.00",
                        'SpotInstanceType': 'one-time',
                        'InstanceInterruptionBehavior': self.interruption_behaviour
                    }
                },
            'Placement': {
                'AvailabilityZone': self.config.availability_zone,
            }
        }

        instances = self.create_instance(parameters)
        created_instances = [i for i in instances]
        return created_instances


    def terminate_instance(self, instance_id):
        try:
            self.ec2_client.terminate_instances(
                InstanceIds = [instance_id]
            )
        except Exception as ex:
            self.log_info(f'Cannot delete instance {instance_id} with error: {ex}')


    def get_spot_hour_rate(self, instance_type, start_time):
        self.log_info(f'Getting spot hour rate for availability zone {self.config.availability_zone}')
        try:
            price_history = self.ec2_client.describe_spot_price_history(
                AvailabilityZone=self.config.availability_zone,
                InstanceTypes=[instance_type],
                MaxResults=1,
                StartTime=start_time
            )
            price_rate = price_history['SpotPriceHistory'][0]['SpotPrice']
            self.log_info(f'Found spot hour rate in availability zone {self.config.availability_zone}: {price_rate}')
            return price_rate
        except Exception as ex:
            self.log_info(f'Cannot retrieve spot history with error: {ex}')