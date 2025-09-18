class GlobusCommandBuilder:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret


    def build_authenticate_command(self):
        return f'export GLOBUS_COMPUTE_CLIENT_ID="{self.client_id}" && export GLOBUS_COMPUTE_CLIENT_SECRET="{self.client_secret}" && python3 endpoint/authenticator.py'


    def build_start_endpoint(self, endpoint_name):
        return self.build_authenticate_command() + self.add_command() + f'/home/ubuntu/.local/bin/globus-compute-endpoint start {endpoint_name}'


    def build_configure_endpoint(self, endpoint_name):
        return self.build_authenticate_command() + self.add_command() + f'/home/ubuntu/.local/bin/globus-compute-endpoint configure {endpoint_name}'


    def build_list_endpoint(self):
        return self.build_authenticate_command() + self.add_command() + f'/home/ubuntu/.local/bin/globus-compute-endpoint list'


    def build_stop_endpoint(self, endpoint_name):
        return self.build_authenticate_command() + self.add_command() + f'/home/ubuntu/.local/bin/globus-compute-endpoint stop {endpoint_name}'


    def build_delete_endpoint(self, endpoint_uuid):
        return self.build_authenticate_command() + self.add_command() + f'/home/ubuntu/.local/bin/globus-compute-endpoint delete --force --yes {endpoint_uuid}'


    def build_cat_endpoint_file(self, endpoint_name):
        return f"cat /home/ubuntu/.globus_compute/{endpoint_name}/endpoint.json"


    def add_command(self):
        return ' && '