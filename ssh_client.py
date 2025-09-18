import paramiko

from base_class import BaseClass


class SshClient(BaseClass):
    def __init__(self, pool_number, connect_retries=5):
        BaseClass.__init__(self, pool_number)
        self.configure_logging()
        self.user = "ubuntu"
        self.port = 22
        self.connect_retries=connect_retries
        self.client = self.initiate_client()
        self.key_file = paramiko.RSAKey.from_private_key_file(self.config.rsa_file_path)
        print("SshClient initialized with pool number:", pool_number)

    
    def initiate_client(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.load_system_host_keys()
        return ssh_client


    def get_output(self, channel):
        outdata = bytes()
        errdata = bytes()
        try:
            while True:
                while channel.recv_ready():
                    outdata += channel.recv(1000)
                    print(outdata)
                while channel.recv_stderr_ready():
                    errdata += channel.recv_stderr(1000)
                if channel.exit_status_ready():
                    break
            retcode = channel.recv_exit_status()
        except Exception as e:
            self.logger.error("<SSH Client> Get output error: except")
            self.logger.error(e)
            raise
        return outdata.decode('utf-8'), errdata.decode('utf-8'), retcode


    def execute_ssh_command(self, command_to_execute, hostname):
        self.log_info(f'Executing ssh command: {command_to_execute} on host: {hostname}')

        retry = 1
        try:
            while retry < self.connect_retries:
                try:
                    self.client.connect(hostname=hostname, username="ubuntu", port=22, timeout=30, pkey=self.key_file, allow_agent=False, look_for_keys=False)
                    break
                except Exception as ex:
                    self.log_info(f'could not connect, retry, err message: {ex}')
                retry += 1
        except Exception as ex:
            self.log_info(f'Could not connect to VM, error message: {ex}')
            raise ConnectionError(f'Could not connect to VM, error message: {ex}')

        ssh_transp = self.client.get_transport()
        channel = ssh_transp.open_session()
        channel.setblocking(0)
        try:
            channel.get_pty()
            channel.exec_command(command_to_execute)
            output, err, status = self.get_output(channel)
            if (len(output) > 0):
                self.log_info(f'OUTPUT: {output}')
            if (len(err) > 0):
                self.log_info(f'ERROR: {err}')
            self.log_info(f'STATUS: {status}')
            return output, err, status
        except Exception as ex:
            self.log_info(f'Could not execute command, error message: {ex}')