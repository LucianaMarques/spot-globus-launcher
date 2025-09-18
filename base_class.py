from datetime import datetime, timezone
import logging
from pathlib import Path

from config import Config


class BaseClass:
    def __init__(self, pool_number):
        self.config = Config()
        self.pool_number = pool_number
        Path(self.config.logging_folder).mkdir(parents=True, exist_ok=True)
        self.file_name = self.config.logging_folder + self.config.logging_file_prefix + f"-{datetime.now(timezone.utc).day}-{datetime.now(timezone.utc).month}-{datetime.now(timezone.utc).hour}-{datetime.now(timezone.utc).minute}-{datetime.now(timezone.utc).second}-pool-{self.pool_number+1}.log"


    def configure_logging(self):
        logging.basicConfig(filename=self.file_name, level=logging.INFO, format='%(asctime)s %(levelname)s %(filename)s %(funcName)s %(message)s')
        self.logger = logging.getLogger(__name__)


    def log_info(self, message):
        if (self.logger == None):
            raise Exception(f"No logger configured for pool: {self.pool_number+1}")
        self.logger.info(f'Pool #{self.pool_number+1}: {message}')