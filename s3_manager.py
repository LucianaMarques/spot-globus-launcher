from base_class import BaseClass
from ssh_client import SshClient


class S3Manager(BaseClass):
    def __init__(self, pool_number, hostname, bucket_name, end_time):
        BaseClass.__init__(self, pool_number)
        self.configure_logging()
        self.pool_number = pool_number
        self.hostname = hostname
        self.bucket_name = bucket_name
        self.file_path = f"/home/ubuntu/.globus_compute/endpoint-6{self.pool_number+1}/tasks_working_dir/masa_function_output.log"
        self.dest_path = f"{end_time.day}-{end_time.month}-{end_time.year}/{self.config.file_name_prefix}/{self.pool_number+1}/{end_time.hour}-{end_time.minute}-{end_time.second}/"
        self.ssh_client = SshClient(pool_number)


    def copy_log_files_to_s3(self):
        self.log_info(f"Copying files to S3 bucket {self.bucket_name} in destination path {self.dest_path}")
        command = f"aws s3 cp {self.file_path} s3://{self.bucket_name}/{self.dest_path}"
        try:
            self.ssh_client.execute_ssh_command(command, self.hostname)
        except Exception as ex:
            self.log_info(f"Could not copy files to S3 with exception message: {ex}")
