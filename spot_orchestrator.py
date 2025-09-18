import boto3
from datetime import datetime, timezone
import time

from base_class import BaseClass
from spot_manager import SpotManager
from s3_manager import S3Manager

class SpotOrchestrator(BaseClass):
    def __init__(self, pool_number, created_instance_id=None):
        BaseClass.__init__(self, pool_number)
        self.configure_logging()
        self.launcher = SpotManager(self.pool_number)
        self.function_executor = None
        self.ec2_resource = boto3.resource('ec2')
        self.maximum_endpoint_creation_retry = 10
        self.endpoint_name=f"endpoint-6{pool_number+1}"
        self.instance_type = self.config.instance_type
        self.bag_size = self.config.bag_size
        # To be filled upon instance and globus endpoint creation
        self.created_instance_id = created_instance_id
        self.endpoint_id = None
        print("SpotOrchestrator initialized with pool number:", pool_number)


    def initialize_instance(self, instance_type):
        try:
            self.log_info(f'Instances initialization')
            created_instances = self.launcher.create_preemptible_instance(instance_type)
            if created_instances is not None:
                self.log_info(f'Created {len(created_instances)} instances.')
            self.created_instance_id = created_instances[0].instance_id

        except Exception as ex:
            self.logger.error(f'Unable to initialize instances of type {instance_type} with error: {ex}')
            raise


    def get_instance_resource(self):
        if self.created_instance_id is None:
            return None
        instance = self.ec2_resource.Instance(self.created_instance_id)
        instance.reload()
        return instance


    def get_instance_hostname(self):
        instance = self.get_instance_resource()
        return instance.public_dns_name
    

    def get_instance_private_dns(self):
        instance = self.get_instance_resource()
        return instance.private_dns_name


    def terminate_instance(self, exceptionMessage=None):
        if exceptionMessage is not None:
            self.log_info(f'Exception occurred: {exceptionMessage}. Terminating instance with id: {self.created_instance_id}')
        try:
            self.log_info(f'Terminating instance with id: {self.created_instance_id}')
            self.launcher.terminate_instance(self.created_instance_id)
            self.log_info(f'Finished terminating instance with id: {self.created_instance_id}')
        except Exception as ex:
            self.log_info(f"Could not terminate instance with id {self.created_instance_id} with error message: {ex}")
            raise


    def get_instance_status(self):
        instance = self.get_instance_resource()
        if instance is None:
            return None
        else:
            return instance.state["Name"].lower()


    def wait_for_instance_started(self):
        self.log_info(f'Waiting for instance {self.created_instance_id} to start')
        while(True):
            instance_status = self.get_instance_status()
            if (instance_status == 'running'):
                break
        time.sleep(60)
        self.log_info(f'Instance {self.created_instance_id} of type {self.instance_type} initialized')


    def copy_logs_to_s3(self, hostname, end_time, endpoint_id):
        self.log_info(f'Copying logs to s3')
        s3_manager = S3Manager(self.pool_number, hostname, self.config.bucket_name, end_time)
        s3_manager.copy_log_files_to_s3()


    def start_and_get_spot_instance(self):
        self.log_info(f'Start time of pool number #{self.pool_number+1}: {datetime.now(timezone.utc)}')
        if self.created_instance_id is None:
            self.initialize_instance(self.instance_type)
            self.wait_for_instance_started()
        self.hostname = self.get_instance_hostname()
        self.private_dns_name = self.get_instance_private_dns()
        self.log_info(f'Found instances hostname: {self.hostname}')

    def copy_files_to_s3(self, end_time):
        self.log_info(f'Finalized experiment at: {end_time}')
        self.copy_logs_to_s3(self.hostname, end_time, self.endpoint_id)


    def calculate_experiment_cost(self, end_time):
        instance = self.get_instance_resource()
        spot_launch_time = instance.launch_time
        elapsed_time = end_time - spot_launch_time
        self.log_info(f'Total elapsed time: {elapsed_time}')
        spot_hour_rate = self.launcher.get_spot_hour_rate(instance_type=self.instance_type, start_time=spot_launch_time)
        experiment_cost = (elapsed_time.total_seconds() * float(spot_hour_rate)) / 3600
        self.log_info(f"Total Cost of the Experiment: {experiment_cost}")
        return experiment_cost