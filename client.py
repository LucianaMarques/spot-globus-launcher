from datetime import datetime, timezone
from multiprocessing import Pool
import time

from GlobusManager.globus_manager import GlobusManager
from spot_orchestrator import SpotOrchestrator
from config import Config


config = Config()


def execute_orchestrator(pool_number):
    print(f'Starting orchestrator for pool {pool_number+1}')
    orchestrator = SpotOrchestrator(pool_number)
    globus_manager = GlobusManager(pool_number)
    endpoint_name=f"endpoint-6{pool_number+1}"
    try:
        orchestrator.start_and_get_spot_instance()
        # globus_manager.configure_endpoint(endpoint_name, orchestrator.created_instance_id, orchestrator.hostname)
        # time.sleep(10)
        
        # for i in range(10):
            # try:
                # print(f'Pool: {pool_number+1}: attempt {i+1} to start {endpoint_name}')
                # output, err, status = globus_manager.start_endpoint(endpoint_name, orchestrator.hostname)
                # time.sleep(10)
                # print(f'Pool {pool_number+1} Status: {status}')
                # print(f'Pool {pool_number+1} Err: {err}')
                # print(f'Pool {pool_number+1} Output: {output}')
                # if 'is already active' in output:
                    # print(f'Pool {pool_number+1}: endpoint {endpoint_name} started successfully')
                    # break
                # if 'Starting endpoint; registered ID:' in output:
                    # print(f'Pool {pool_number+1}: endpoint {endpoint_name} started successfully')
                    # break
            # except:
                # print(f'Pool: {pool_number+1}: could not start {endpoint_name}')
        
        # endpoint_uuid = globus_manager.get_endpoint_uuid(orchestrator.private_dns_name, endpoint_name)
        # globus_manager.execute_function(endpoint_uuid)
        
        # end_time = datetime.now(timezone.utc)
        # orchestrator.copy_files_to_s3(end_time)
        
        # globus_manager.stop_endpoint(endpoint_uuid)
        # time.sleep(10)
        # globus_manager.delete_endpoint(endpoint_uuid)
        
        # orchestrator.calculate_experiment_cost(end_time)

        # if (orchestrator.created_instance_id != None):
            # orchestrator.terminate_instance()

        return f'Pool {pool_number+1}: finished successfully, endpoint {pool_number+1} instance id: {orchestrator.created_instance_id}'
    except Exception as ex:
        if (orchestrator.created_instance_id != None):
            orchestrator.terminate_instance(exceptionMessage=str(ex))
        print(f'Task #{pool_number + 1} failed with exception message: {ex}')
        return 1

if __name__ == '__main__':
    if (config.num_bags == 1):
        result = execute_orchestrator(0)
        print(result)
    else:
        with Pool(config.num_bags) as pool:
            results = pool.imap_unordered(execute_orchestrator, range(0, config.num_bags))
            for result in results:
                print(result)