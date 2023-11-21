/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/config/configurator_define.h"

namespace indexlib { namespace config {

const std::string TABLE_NAME = "table_name";
const std::string TABLE_TYPE = "table_type";
const std::string TABLE_TYPE_INDEX = "normal";
const std::string TABLE_TYPE_LINEDATA = "line_data";
const std::string TABLE_TYPE_RAWFILE = "raw_file";
const std::string TABLE_TYPE_KV = "kv";
const std::string TABLE_TYPE_KKV = "kkv";
const std::string TABLE_TYPE_CUSTOMIZED = "customized";
const std::string TABLE_TYPE_ORC = "orc";
const std::string SCHEMA_VERSION_ID = "versionid";
const std::string MAX_SCHEMA_MODIFY_OPERATION_COUNT = "max_modify_operation_count";
const std::string SCHEMA_MODIFY_OPERATIONS = "modify_operations";
const std::string SCHEMA_MODIFY_ADD = "add";
const std::string SCHEMA_MODIFY_DEL = "delete";
const std::string SCHEMA_MODIFY_PARAMETER = "parameters";

const std::string TABLE_USER_DEFINED_PARAM = "user_defined_param";
const std::string GLOBAL_REGION_INDEX_PREFERENCE = "global_region_index_preference";

const std::string TABLES = "tables";

const std::string DICTIONARIES = "dictionaries";
const std::string DICTIONARY_NAME = "dictionary_name";
const std::string DICTIONARY_CONTENT = "content";

const std::string ADAPTIVE_DICTIONARIES = "adaptive_dictionaries";
const std::string ADAPTIVE_DICTIONARY_NAME = "adaptive_dictionary_name";
const std::string ADAPTIVE_DICTIONARY_TYPE = "dict_type";
const std::string ADAPTIVE_DICTIONARY_THRESHOLD = "threshold";

const std::string FIELDS = "fields";
const std::string FIELD_NAME = "field_name";
const std::string FIELD_TYPE = "field_type";
const std::string FIELD_VALID_VALUES = "valid_values";
const std::string FIELD_MULTI_VALUE = "multi_value";
const std::string FIELD_ANALYZER = "analyzer";
const std::string ATTRIBUTE_UNIQ_ENCODE = "uniq_encode";
const std::string ATTRIBUTE_UPDATABLE_MULTI_VALUE = "updatable_multi_value";
const std::string FIELD_BINARY = "binary_field";
const std::string FIELD_USER_DEFINED_PARAM = "user_defined_param";
// const std::string ATTRIBUTE_U32OFFSET_THRESHOLD = "u32offset_threshold";
const std::string FIELD_VIRTUAL = "virtual";
const std::string FIELD_DEFAULT_VALUE = "default_value";
const std::string FIELD_DEFAULT_STR_VALUE = "";
const std::string FIELD_SEPARATOR = "separator";

const std::string FIELD_SUPPORT_NULL = "enable_null";
const std::string FIELD_IS_BUILT_IN = "built_in_field";
const std::string FIELD_DEFAULT_NULL_STRING_VALUE = "default_null_string";
const std::string FIELD_USER_DEFINE_ATTRIBUTE_NULL_VALUE = "user_define_attribute_null_value";

const std::string FIELD_DEFAULT_TIME_ZONE = "default_time_zone";

// const std::string COMPRESS_TYPE = "compress_type";
const std::string COMPRESS_UNIQ = "uniq";
const std::string COMPRESS_EQUAL = "equal";
const std::string COMPRESS_BLOCKFP = "block_fp";
const std::string COMPRESS_FP16 = "fp16";
const std::string COMPRESS_PATCH = "patch_compress";
const std::string COMPRESS_INT8_PREFIX = "int8#";
// const std::string FILE_COMPRESS = "file_compress";
// const std::string FILE_COMPRESS_SCHEMA = "file_compress";

const std::string PAYLOAD_NAME = "payload_name";
const std::string PAYLOAD_ATOMIC_UPDATE = "atomic_update";
const std::string PAYLOAD_FIELDS = "payload_fields";

const std::string SOURCE = "source";
const std::string INDEXS = "indexs";
const std::string INDEX_NAME = "index_name";
const std::string INDEX_ANALYZER = "index_analyzer";
const std::string INDEX_TYPE = "index_type";
const std::string INDEX_FORMAT_VERSIONID = "format_version_id";
const std::string INDEX_FIELDS = "index_fields";
const std::string FIELD_BOOST = "boost";
const std::string NEED_SHARDING = "need_sharding";
const std::string SHARDING_COUNT = "sharding_count";
const std::string TERM_PAYLOAD_FLAG = "term_payload_flag";
const std::string DOC_PAYLOAD_FLAG = "doc_payload_flag";
const std::string POSITION_PAYLOAD_FLAG = "position_payload_flag";
const std::string POSITION_LIST_FLAG = "position_list_flag";
const std::string TERM_FREQUENCY_FLAG = "term_frequency_flag";
const std::string TERM_FREQUENCY_BITMAP = "term_frequency_bitmap";
const std::string HAS_PRIMARY_KEY_ATTRIBUTE = "has_primary_key_attribute";
const std::string HIGH_FEQUENCY_DICTIONARY = "high_frequency_dictionary";
const std::string HIGH_FEQUENCY_ADAPTIVE_DICTIONARY = "high_frequency_adaptive_dictionary";
const std::string HIGH_FEQUENCY_TERM_POSTING_TYPE = "high_frequency_term_posting_type";
const std::string HAS_SECTION_ATTRIBUTE = "has_section_attribute";
const std::string SECTION_ATTRIBUTE_CONFIG = "section_attribute_config";
const std::string HAS_SECTION_WEIGHT = "has_section_weight";
const std::string HAS_SECTION_FIELD_ID = "has_field_id";
const std::string MAX_FIRSTOCC_IN_DOC = "max_firstocc_in_doc";
const std::string HIGH_FREQUENCY_DICTIONARY_SELF_ADAPTIVE_FLAG = "high_frequency_dictionary_self_adaptive_flag";
const std::string HAS_TRUNCATE = "has_truncate";
const std::string HAS_SHORTLIST_VBYTE_COMPRESS = "has_shortlist_vbyte_compress";
const std::string INDEX_COMPRESS_MODE = "compress_mode";
const std::string INDEX_COMPRESS_MODE_PFOR_DELTA = "pfordelta";
const std::string INDEX_COMPRESS_MODE_REFERENCE = "reference";
const std::string USE_TRUNCATE_PROFILES = "use_truncate_profiles";
const std::string USE_TRUNCATE_PROFILES_SEPRATOR = ";";
const std::string USE_HASH_DICTIONARY = "use_hash_typed_dictionary";
const std::string TRUNCATE_INDEX_NAME_MAPPER = "truncate_index_name_mapper";
const std::string USE_NUMBER_PK_HASH = "use_number_pk_hash";
const std::string PRIMARY_KEY_STORAGE_TYPE = "pk_storage_type";
const std::string INDEX_UPDATABLE = "index_updatable";
const std::string PATCH_COMPRESSED = "patch_compressed";
const std::string SORT_FIELD = "sort_field";

// customize index
const std::string CUSTOM_DATA_DIR_NAME = "custom";
const std::string CUSTOMIZED_INDEXER_NAME = "indexer";
const std::string CUSTOMIZED_INDEXER_PARAMS = "parameters";

// spatial index
const std::string MAX_SEARCH_DIST = "max_search_dist";
const std::string MAX_DIST_ERROR = "max_dist_err";
const std::string DIST_LOSS_ACCURACY = "distance_loss_accuracy";
const std::string DIST_CALCULATOR = "dist_calculator";

const std::string CALCULATOR_HAVERSINE = "haversine";

const std::string ATTRIBUTES = "attributes";
const std::string ATTRIBUTE_EXTEND = "extend";
const std::string ATTRIBUTE_EXTEND_SORT = "sort";
const std::string SORT_BY_WEIGHT_TAG = "sort";
const std::string ATTRIBUTE_NAME = "attribute_name";
// const std::string PACK_ATTR_NAME_FIELD = "pack_name";
// const std::string PACK_ATTR_SUB_ATTR_FIELD = "sub_attributes";
// const std::string PACK_ATTR_COMPRESS_TYPE_FIELD = "compress_type";
// const std::string PACK_ATTR_VALUE_FORMAT = "value_format";
// const std::string PACK_ATTR_VALUE_FORMAT_IMPACT = "impact";
// const std::string PACK_ATTR_VALUE_FORMAT_PLAIN = "plain";

const std::string SUMMARYS = "summarys";
const std::string SUMMARY_FIELDS = "summary_fields";
const std::string SUMMARY_COMPRESS = "compress";
const std::string SUMMARY_COMPRESS_BLOCK_SIZE = "compress_block_size";
// const std::string SUMMARY_COMPRESS_TYPE = "compress_type";
const std::string SUMMARY_GROUPS = "summary_groups";
const std::string SUMMARY_GROUP_NAME = "group_name";
const std::string SUMMARY_GROUP_PARAMTETER = "parameter";
const std::string SUMMARY_ADAPTIVE_OFFSET = "adaptive_offset";

const std::string REGIONS = "regions";
const std::string REGION_NAME = "region_name";

const std::string ENABLE_TTL = "enable_ttl";
const std::string DEFAULT_TTL = "default_ttl";
const std::string TTL_FIELD_NAME = "ttl_field_name";
const std::string TTL_FROM_DOC = "ttl_from_doc";
const std::string AUTO_UPDATE_PREFERENCE = "auto_update_preference";
const std::string INSERT_OR_IGNORE = "insert_or_ignore";

const std::string ENABLE_TEMPERATURE_LAYER = "enable_temperature_layer";
const std::string TEMPERATURE_LAYER_CONFIG = "temperature_layer_config";

const std::string ENABLE_HASH_ID = "enable_hash_id";
const std::string HASH_ID_FIELD_NAME = "hash_id_field_name";

const std::string ORDER_PRESERVING_FIELD = "order_preserving_field";

const std::string SUB_SCHEMA = "sub_schema";

const std::string TRUNCATE_PROFILES = "truncate_profiles";
const std::string TRUNCATE_PROFILE_NAME = "truncate_profile_name";
const std::string TRUNCATE_SORT_DESCRIPTIONS = "sort_descriptions";
const std::string TRUNCATE_DOC_PAYLOAD_PARAMETERS = "docpayload_parameters";
const std::string TRUNCATE_PAYLOAD_NAME = "payload_name";

const std::string ANALYZERS = "analyzers";
const std::string ANALYZER_NAME = "analyzer_name";
const std::string TOKENIZER_CONFIGS = "tokenizer_configs";
const std::string STOPWORDS = "stopwords";
const std::string NORMALIZE_OPTIONS = "normalize_options";

const std::string MASTER = "master";
const std::string SLAVE = "slave";
const std::string SCHEMA_NAME = "schema_name";

const std::string HIGH_FREQ_TERM_BOTH_POSTING = "both";
const std::string HIGH_FREQ_TERM_BITMAP_POSTING = "bitmap";

const std::string DESC_SORT_PATTERN = "DESC";
const std::string ASC_SORT_PATTERN = "ASC";

const std::string DEFAULT_TRUNCATE_STRATEGY_TYPE = "default";
const std::string TRUNCATE_META_STRATEGY_TYPE = "truncate_meta";

const std::string INDEX_PREFERENCE_NAME = "index_preference";
const std::string USE_NUMBER_HASH = "use_number_hash";
const std::string PREFERENCE_TYPE_NAME = "type";
const std::string PERF_PREFERENCE_TYPE = "PERF";
const std::string STORE_PREFERENCE_TYPE = "STORE";
const std::string PREFERENCE_PARAMS_NAME = "parameters";

const std::string KKV_HASH_PARAM_NAME = "hash_dict";
const std::string KKV_SKEY_PARAM_NAME = "suffix_key";
const std::string SUFFIX_KEY_WRITER_LIMIT = "suffix_key_writer_limit";
const std::string KKV_VALUE_PARAM_NAME = "value";
const std::string KKV_VALUE_INLINE = "value_inline";

const std::string KKV_BUILD_PROTECTION_THRESHOLD = "build_protection_threshold";

const std::string KV_HASH_PARAM_NAME = "hash_dict";
const std::string KV_VALUE_PARAM_NAME = "value";
const std::string BUILD_GRANULARITY = "build_granularity";
const std::string SEARCH_GRANULARITY = "search_granularity";

// customized config
const std::string CUSTOMIZED_CONFIG = "customized_config";
const std::string CUSTOMIZED_TABLE_CONFIG = "customized_table_config";
const std::string CUSTOMIZED_PLUGIN_IDENTIFIER = "identifier";
const std::string CUSTOMIZED_DOCUMENT_CONFIG = "customized_document_config";
const std::string CUSTOMIZED_PLUGIN_NAME = "plugin_name";
const std::string CUSTOMIZED_PARAMETERS = "parameters";
const std::string CUSTOMIZED_IDENTIFIER = "id";

const std::string CUSTOMIZED_DOCUMENT_CONFIG_RAWDOCUMENT = "document.raw_document";
const std::string CUSTOMIZED_DOCUMENT_CONFIG_PARSER = "document.parser";

const std::string SECTION_DIR_NAME_SUFFIX = "_section";

// source config
const std::string SOURCE_GROUP_CONFIG_FIELD_MODE = "field_mode";
const std::string SOURCE_GROUP_CONFIG_FIELDS = "fields";
const std::string SOURCE_GROUP_CONFIG_PARAMETER = "parameter";

const std::string SOURCE_GROUP_CONFIGS = "group_configs";

const std::string TABLET = "tablet";
}} // namespace indexlib::config
