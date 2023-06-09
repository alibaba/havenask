#######################################################
#parameter used by tools
#######################################################
BUILD_APP_CONFIG_FILE_NAME = 'build_app.json'
BUILD_APP_USER_NAME = 'user_name'
BUILD_APP_SERVICE_NAME = 'service_name'
BUILD_APP_NUWA_ADDRESS = 'nuwa_service_ip'
BUILD_APP_APSARA_PACKAGE_NAME = 'apsara_package_name'
BUILD_APP_HADOOP_PACKAGE_NAME = 'hadoop_package_name'
BUILD_APP_INDEX_ROOT = 'index_root'
BUILD_APP_ZOOKEEPER_ROOT = 'zookeeper_root'
BUILD_APP_HIPPO_ROOT = 'hippo_root'
BUILD_APP_AMONITOR_PORT = 'amon_agent_port'

BUILD_CLUSTERS_DIR_NAME = 'clusters'
BUILD_CLUSTERS_ZIP_FILE_NAME = 'clusters.zip'
BUILD_CLUSTER_FILE_SUFFIX = '_cluster.json'

DATA_TABLES_DIR_NAME = 'data_tables'
DATA_TABLE_FILE_SUFFIX = '_table.json'

BUILD_RULE_PARTITION_COUNT = 'partition_count'
BUILD_RULE_BUILD_PARALLEL_NUM = 'build_parallel_num'
BUILD_RULE_MERGE_PARALLEL_NUM = 'merge_parallel_num'
BUILD_RULE_PARTITION_SPLIT_NUM = 'partition_split_num'
BUILD_RULE_MAP_REDUCE_RATIO = 'map_reduce_ratio'
BUILD_RULE_NEED_PARTITION = 'need_partition'

# build step
JOB_BUILD_STEP = 'build'
JOB_MERGE_STEP = 'merge'
JOB_END_MERGE_STEP = 'end_merge'
JOB_BOTH_STEP = 'both'

# build mode
BUILD_MODE = 'build_mode'
BUILD_MODE_FULL = 'full'
BUILD_MODE_INC = 'incremental'

# job type
JOB_TYPE_APSARA = 'apsara'
JOB_TYPE_HADOOP = 'hadoop'
JOB_TYPE_LOCAL = 'local'

# job status
JOB_STATUS_RUNNING = 'Running'
JOB_STATUS_WAITING = 'Waiting'
JOB_STATUS_FAILED = 'Failed'
JOB_STATUS_SUCCESS = 'Success'
JOB_STATUS_NOT_RUNNING = 'NotRunning'
JOB_STATUS_UNKNOWN = 'Unknown'

# kv config
CONFIG_PATH = 'config'
DATA_PATH = 'data'
GENERATION = 'generation'
CLUSTER_NAME = 'cluster_names'
BUILD_PART_FROM = 'build_partition_from'
BUILD_PART_COUNT = 'build_partition_count'
INDEX_ROOT_PATH = 'index_root'
FINAL_INDEX_ROOT_PATH = 'final_index_root'
SRC_SIGNATURE = 'src_signature'
MERGE_CONFIG_NAME = 'merge_config_name'
READ_SRC = 'src'
READ_SRC_TYPE = 'type'
DOCUMENT_FORMAT = 'document_format'
RAW_DOC_SCHEMA_NAME = 'raw_doc_schema_name'
DATA_DESCRIPTION = 'data_description'
MERGE_TIMESTAMP = 'merge_timestamp'
FILE_READ_SRC = 'file'
HA3_DOCUMENT_FORMAT = 'ha3'
ISEARCH_DOCUMENT_FORMAT = 'isearch'

# hadoop
HADOOP_NLINE_INPUT_FORMAT = ' org.apache.hadoop.mapred.lib.NLineInputFormat '

HADOOP_BUILD_EXE = 'bs_hadoop_build_task'
HADOOP_MERGE_EXE = 'bs_hadoop_merge_task'
HADOOP_END_MERGE_EXE = 'bs_hadoop_end_merge_task'

HADOOP_BUILD_JOB_TEMPLATE = 'hadoop_build_job.xml'
HADOOP_MERGE_JOB_TEMPLATE = 'hadoop_merge_job.xml'

# tablet
TABLET_MODE = "tablet_mode"
