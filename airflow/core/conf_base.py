from airflow.etc.const import DRIVER_CRATE_CLASS

from airflow.libs.logging_helper import LoggerManager
from airflow.libs.postgres_lib import UidInfoDB
from airflow.libs.redis_lib import UidCache


class ConfBase(object):
    def __init__(self, source_id, in_file, in_ds, in_dict_params, num_in_partition=10, num_out_partition=1):
        self.source_id = source_id
        self.in_file = in_file
        self.num_in_partition = num_in_partition
        self.num_out_partition = num_out_partition
        self.ds = in_ds
        self.dict_params = in_dict_params
        self.in_path = in_dict_params.get("in_path")
        self.skip_empty = in_dict_params.get("skip_empty", False)