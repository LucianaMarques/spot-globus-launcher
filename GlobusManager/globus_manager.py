from datetime import datetime
from globus_compute_sdk import Client, Executor
from globus_compute_sdk.serialize import CombinedCode, ComputeSerializer

from base_class import BaseClass
from GlobusManager.globus_command_builder import GlobusCommandBuilder
from ssh_client import SshClient


def execute_masa_function_in_endpoint(supertask_size, verbose_log=True):
    import logging
    import subprocess
    import os
    log_file_name = f"masa_function_output.log"
    logging.basicConfig(filename=log_file_name, level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    os.chdir("./")
    command = "/home/ubuntu/endpoint/MASA-OpenMP/masa-openmp-1.0.1.1024/masa-openmp /home/ubuntu/endpoint/sequence.fasta /home/ubuntu/endpoint/sequence2.fasta"
    for i in range(0, supertask_size):
        r = subprocess.run(command.split(), capture_output=True)
        logger.info(f'Executed run {i+1} with return code: {r.returncode}')
        if (verbose_log):
            logger.info(f'Executed run {i+1} with stdout: {r.stdout}')
    return r.returncode


def execute_sum_of_vector(size_in_mb, num_workers):
    import logging
    import os
    import numpy as np
    import random
    import multiprocess as mp
    import time

    log_file_name = f"masa_function_output.log"
    logging.basicConfig(filename=log_file_name, level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    
    def task(index, start, end, memory_block):
        log_file_name = f"masa_function_output.log"
        logging.basicConfig(filename=log_file_name, level=logging.DEBUG)
        logger = logging.getLogger(__name__)
        startTime = time.time()
        logger.info(f"Starting task #{index}, start: {start}, end: {end}")
        logger.info(f"Start time of task #{index}: {startTime}")
        i = start
        while(i <= end):
            memory_block[i] = random.randint(1, 10)
            i += 1
        endTime = time.time()
        logger.info(f"End time of task #{index}: {endTime}")

    os.chdir("./")
    print(f"Sum of vector: {size_in_mb + num_workers}")

    # 2^10 * 2^10 = 1MB
    # 2^10 * 2^10 * 2^10 = 1GB
    size_in_floats = size_in_mb * (1024 * 1024) // 8
    memory_block = np.zeros(size_in_floats, dtype=np.float64)
    logger.info(f"Vector allocated size: {size_in_mb}, workers: {num_workers}")

    start = 0
    step = size_in_floats // num_workers
    logger.info(f"Diff calculated: {step}")
    end = step
    if (end >= size_in_floats):
        end = size_in_floats -1

    threads = []

    for i in range (num_workers):
        logger.info(f"Starting task #{i}, start: {start}, end: {end}")
        thread = mp.Process(target=task, args=(i, start, end, memory_block,))
        threads.append(thread)
        thread.start()
        start = end + 1
        end += step + 1
        if (end >= size_in_floats):
            end = size_in_floats -1

    for thread in threads:
        thread.join()
    
    logger.info(f"Finished executing tasks")


class GlobusManager(BaseClass):
    def __init__(self, pool_number):
        BaseClass.__init__(self, pool_number)
        self.configure_logging()
        self.ssh_client = SshClient(pool_number)
        self.command_builder = GlobusCommandBuilder(self.config.client_id, self.config.client_secret)
        self.max_retry = 5
        print("Attempting to initialize Globus client")
        self.gcc = Client(code_serialization_strategy=CombinedCode())
        self.gcc.serializer = ComputeSerializer(strategy_code=CombinedCode())
        print("Globus client initialized successfully")
        print("GlobusManager initialized with pool number:", pool_number)


    def configure_endpoint(self, endpoint_name, instance_id, hostname):
        try:
            self.log_info(f'Attempting to configure endpoint "{endpoint_name}" in instance {instance_id}')
            command = self.command_builder.build_configure_endpoint(endpoint_name)
            self.ssh_client.execute_ssh_command(command, hostname)
        except Exception as ex:
            self.log_info(f"Could not configure endpoint {endpoint_name} with error message: {ex}")
            raise


    def get_endpoint_uuid(self, private_dns, endpoint_name):
        endpoints = self.gcc.get_endpoints()
        for endpoint in endpoints:
            if endpoint['name'] == endpoint_name:
                metadata = self.gcc.get_endpoint_metadata(endpoint['uuid'])
                if metadata['hostname'] == private_dns:
                    return endpoint['uuid']
        raise(f'No endpoint found for matching {endpoint_name} and {private_dns}')


    def start_endpoint(self, endpoint_name, hostname):
        try:
            command = self.command_builder.build_start_endpoint(endpoint_name)
            return self.ssh_client.execute_ssh_command(command, hostname)
        except Exception as ex:
            self.log_info(f"Could not start endpoint with name {endpoint_name} with error message: {ex}")
            raise


    def stop_endpoint(self, endpoint_id):
        self.log_info(f'Stopping endpoint: {endpoint_id}')
        try:
            self.gcc.stop_endpoint(endpoint_id)
        except Exception as ex:
            self.log_info(f"Could not stop endpoint {endpoint_id} with error message: {ex}")
            raise


    def delete_endpoint(self, endpoint_id):
        self.log_info(f'Deleting endpoint with id: {endpoint_id}')
        try:
            self.gcc.delete_endpoint(endpoint_id)
        except Exception as ex:
            self.log_info(f"Could not delete endpoint {endpoint_id} with error message: {ex}")
            raise


    def execute_function(self, endpoint_id):
        start_time = datetime.now()
        self.log_info(f'Starting execution for bag of tasks in endpoint: {endpoint_id}')
        self.log_info(f'Bag size: {self.config.bag_size}')
        self.log_info(f'Start timestamp: {start_time}')
        with Executor(endpoint_id=endpoint_id, client=self.gcc) as gce:
            gce.serializer = ComputeSerializer(strategy_code=CombinedCode())
            function_id = gce.register_function(execute_sum_of_vector)
            attempts = 0
            while (attempts < self.max_retry):
                try:
                    self.log_info("Attempting to execute function")
                    # TODO: we need to make this part to be generic for the user's function
                    #future = gce.submit_to_registered_function(function_id, kwargs={ "bag_size": self.config.bag_size })
                    args = { "size_in_mb": 9000, "num_workers": 6 }
                    self.log_info(f'Function args: {args}')
                    future = gce.submit_to_registered_function(function_id, kwargs=args)
                    self.log_info(f'Function result: {future.result()}')
                    break
                except Exception as ex:
                    if (attempts == self.max_retry-1):
                        self.log_info(f'Could not finalize function execution, error message: {ex}')
                        raise
                    else:
                        self.log_info(f'Attempting again as function execution failed with: {ex}')
                        attempts +=1
            self.log_info(f"Finished executing function")