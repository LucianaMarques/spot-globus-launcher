import configparser


class Config:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read("SpotOrchestrator/system.cfg")
        self.spot_launcher = config['spotLauncher']
        self.ami_id = self.spot_launcher['ami_id']
        self.rg_name = self.spot_launcher['resource_group_name']
        self.rsa_file_name = self.spot_launcher['rsa_file_name']
        self.rsa_file_path = f"{self.spot_launcher['rsa_file_path']}/{self.rsa_file_name}.pem"
        self.availability_zone = self.spot_launcher['availability_zone']
        self.iam_arn = self.spot_launcher['iam_arn']
        self.bucket_name = self.spot_launcher['bucket_name']
        
        self.ssh = config['ssh']
        self.client_id = self.ssh['client_id']
        self.client_secret = self.ssh['client_secret']

        self.bag_of_tasks = config['BagOfTasks']
        self.instance_type = self.bag_of_tasks['instance_type']
        self.bag_size = int(self.bag_of_tasks['bag_size'])
        self.num_bags = int(self.bag_of_tasks['num_bags'])

        self.logging = config['logging']
        self.logging_folder = self.logging['logging_folder']
        self.file_name_prefix = self.logging['file_name_prefix']
        self.logging_file_prefix = self.file_name_prefix + f'-{self.instance_type}-{self.bag_size}-bagsize'