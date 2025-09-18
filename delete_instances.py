from argparse import ArgumentParser
import boto3

from GlobusManager.globus_command_builder import GlobusCommandBuilder
from GlobusManager.globus_manager import GlobusManager
from ssh_client import SshClient

parser = ArgumentParser(prog="DeleteInstanceAndGlobusEndpointScript",
                        description="Script for deleting instance and globus endpoint",
                        epilog="Some final message") #TODO: edit epilog

parser.add_argument('--instance_id', '-id', default="", type=str, required=True)
parser.add_argument('--endpoint_name', '-en', default="", type=str, required=True)
parser.add_argument('--client_id', '-ci', type=str, required=True)
parser.add_argument('--client_secret', '-cs', type=str, required=True)
args = parser.parse_args()


ec2_client = boto3.client('ec2')
ec2_resource = boto3.resource('ec2')

hostname = ec2_resource.Instance(args.instance_id).public_dns_name

ssh_client = SshClient(0)
globus_command_builder = GlobusCommandBuilder(args.client_id, args.client_secret)
globus_manager = GlobusManager(0)

try:
    command = globus_command_builder.build_stop_endpoint(args.endpoint_name)
    output, err, status = ssh_client.execute_ssh_command(command, hostname)
    if status != 0:
        print(f'Error executing command: {command}, error message: {err}')
        raise Exception(f'Error executing command: {command}, error message: {err}')
except Exception as ex:
    print(f"Could not stop {args.endpoint_name} in host {hostname} with error message: {ex}")
    raise

print(f'Deleting endpoint with id: {args.endpoint_name}')
try:
    command = globus_command_builder.build_delete_endpoint(args.endpoint_name)
    output, err, status = ssh_client.execute_ssh_command(command, hostname)
    if status != 0:
        print(f'Error executing command: {command}, error message: {err}')
        raise Exception(f'Error executing command: {command}, error message: {err}')
except Exception as ex:
    print(f"Could not delete endpoint {args.endpoint_name} in host {hostname} with error message: {ex}")
    raise

try:
    ec2_client.terminate_instances(
        InstanceIds = [args.instance_id]
    )
    print(f"Deleted instance with id {args.instance_id}")
except Exception as ex:
    print(f'Cannot delete instance {args.instance_id} with error: {ex}')