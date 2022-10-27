#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <limits>
#include <string>

#ifndef __INDEXLIB_CONFIG_CONFIGURATORDEFINE_H
#define __INDEXLIB_CONFIG_CONFIGURATORDEFINE_H

IE_NAMESPACE_BEGIN(config);

static const std::string TABLE_NAME = "table_name";
static const std::string TABLE_TYPE = "table_type";
static const std::string TABLE_TYPE_INDEX = "normal";
static const std::string TABLE_TYPE_LINEDATA = "line_data";
static const std::string TABLE_TYPE_RAWFILE = "raw_file";
static const std::string TABLE_TYPE_KV = "kv";
static const std::string TABLE_TYPE_KKV = "kkv";
static const std::string TABLE_TYPE_CUSTOMIZED = "customized";
static const std::string SCHEMA_VERSION_ID = "versionid";
static const std::string MAX_SCHEMA_MODIFY_OPERATION_COUNT = "max_modify_operation_count";
static const std::string SCHEMA_MODIFY_OPERATIONS = "modify_operations";
static const std::string SCHEMA_MODIFY_ADD = "add";
static const std::string SCHEMA_MODIFY_DEL = "delete";
static const std::string SCHEMA_MODIFY_PARAMETER = "parameters";

static const std::string TABLE_USER_DEFINED_PARAM = "user_defined_param";
static const std::string GLOBAL_REGION_INDEX_PREFERENCE = "global_region_index_preference";

static const std::string TABLES = "tables";

static const std::string DICTIONARIES = "dictionaries";
static const std::string DICTIONARY_NAME = "dictionary_name";
static const std::string DICTIONARY_CONTENT = "content";

static const std::string ADAPTIVE_DICTIONARIES = "adaptive_dictionaries";
static const std::string ADAPTIVE_DICTIONARY_NAME = "adaptive_dictionary_name";
static const std::string ADAPTIVE_DICTIONARY_TYPE = "dict_type";
static const std::string ADAPTIVE_DICTIONARY_THRESHOLD = "threshold";
static const uint32_t INVALID_ADAPTIVE_THRESHOLD = -1;
static const size_t DEFAULT_MAX_VALUE_SIZE_FOR_SHORT_OFFSET =
    (size_t)(std::numeric_limits<uint32_t>::max() - 1);    

static const std::string FIELDS = "fields";
static const std::string FIELD_NAME = "field_name";
static const std::string FIELD_TYPE = "field_type";
static const std::string FIELD_VALID_VALUES = "valid_values";
static const std::string FIELD_MULTI_VALUE = "multi_value";
static const std::string FIELD_ANALYZER = "analyzer";
static const std::string FIELD_UNIQ_ENCODE = "uniq_encode";
static const std::string FIELD_UPDATABLE_MULTI_VALUE = "updatable_multi_value";
static const std::string FIELD_BINARY = "binary_field";
static const std::string FIELD_USER_DEFINED_PARAM = "user_defined_param";
static const std::string FIELD_U32OFFSET_THRESHOLD = "u32offset_threshold";
static const std::string FIELD_VIRTUAL = "virtual";
static const std::string FIELD_DEFAULT_VALUE = "default_value";
static const uint32_t FIELD_U32OFFSET_THRESHOLD_MAX = 0xFFFFFFFFL;
static const std::string FIELD_DEFAULT_STR_VALUE = "";
static const std::string FIELD_DEFRAG_SLICE_PERCENT = "defrag_slice_percent";
static const std::string FIELD_FIXED_MULTI_VALUE_COUNT = "fixed_multi_value_count";
static const uint32_t FIELD_DEFAULT_DEFRAG_SLICE_PERCENT = 50;
static const uint32_t FIELD_NOT_DEFRAG_SLICE_PERCENT = 200;
static const std::string COMPRESS_TYPE = "compress_type";
static const std::string COMPRESS_UNIQ = "uniq";
static const std::string COMPRESS_EQUAL = "equal";
static const std::string COMPRESS_BLOCKFP = "block_fp";
static const std::string COMPRESS_FP16 = "fp16";
static const std::string COMPRESS_PATCH = "patch_compress";
static const std::string COMPRESS_INT8_PREFIX = "int8#";

static const std::string PAYLOAD_NAME = "payload_name";
static const std::string PAYLOAD_ATOMIC_UPDATE = "atomic_update";
static const std::string PAYLOAD_FIELDS = "payload_fields";

static const std::string INDEXS = "indexs";
static const std::string INDEX_NAME = "index_name";
static const std::string INDEX_ANALYZER = "index_analyzer";
static const std::string INDEX_TYPE = "index_type";
static const std::string INDEX_FIELDS = "index_fields";
static const std::string FIELD_BOOST = "boost";
static const std::string NEED_SHARDING = "need_sharding";
static const std::string SHARDING_COUNT = "sharding_count";
static const std::string TERM_PAYLOAD_FLAG = "term_payload_flag";
static const std::string DOC_PAYLOAD_FLAG = "doc_payload_flag";
static const std::string POSITION_PAYLOAD_FLAG = "position_payload_flag";
static const std::string POSITION_LIST_FLAG = "position_list_flag";
static const std::string TERM_FREQUENCY_FLAG = "term_frequency_flag";
static const std::string TERM_FREQUENCY_BITMAP = "term_frequency_bitmap";
static const std::string HAS_PRIMARY_KEY_ATTRIBUTE = "has_primary_key_attribute";
static const std::string HIGH_FEQUENCY_DICTIONARY = "high_frequency_dictionary";
static const std::string HIGH_FEQUENCY_ADAPTIVE_DICTIONARY = "high_frequency_adaptive_dictionary";
static const std::string HIGH_FEQUENCY_TERM_POSTING_TYPE = "high_frequency_term_posting_type";
static const std::string HAS_SECTION_ATTRIBUTE = "has_section_attribute";
static const std::string SECTION_ATTRIBUTE_CONFIG = "section_attribute_config";
static const std::string HAS_SECTION_WEIGHT = "has_section_weight";
static const std::string HAS_SECTION_FIELD_ID = "has_field_id";
static const std::string MAX_FIRSTOCC_IN_DOC = "max_firstocc_in_doc";
static const std::string HIGH_FREQUENCY_DICTIONARY_SELF_ADAPTIVE_FLAG = "high_frequency_dictionary_self_adaptive_flag";
static const std::string HAS_TRUNCATE = "has_truncate";
static const std::string HAS_DICT_INLINE_COMPRESS = "has_dict_inline_compress";
static const std::string INDEX_COMPRESS_MODE = "compress_mode";
static const std::string INDEX_COMPRESS_MODE_PFOR_DELTA = "pfordelta";
static const std::string INDEX_COMPRESS_MODE_REFERENCE = "reference";
static const std::string USE_TRUNCATE_PROFILES = "use_truncate_profiles";
static const std::string USE_TRUNCATE_PROFILES_SEPRATOR = ";";
static const std::string USE_HASH_DICTIONARY = "use_hash_typed_dictionary";
static const std::string TRUNCATE_INDEX_NAME_MAPPER = "truncate_index_name_mapper";
static const std::string USE_NUMBER_PK_HASH = "use_number_pk_hash";
static const std::string PRIMARY_KEY_STORAGE_TYPE = "pk_storage_type";

//customize index
static const std::string CUSTOM_DATA_DIR_NAME = "custom";
static const std::string CUSTOMIZED_INDEXER_NAME = "indexer";
static const std::string CUSTOMIZED_INDEXER_PARAMS = "parameters";

//spatial index
static const std::string MAX_SEARCH_DIST = "max_search_dist";
static const std::string MAX_DIST_ERROR = "max_dist_err";
static const std::string DIST_LOSS_ACCURACY = "distance_loss_accuracy";
static const std::string DIST_CALCULATOR = "dist_calculator";

static const std::string CALCULATOR_HAVERSINE = "haversine";

static const std::string ATTRIBUTES = "attributes";
static const std::string ATTRIBUTE_EXTEND = "extend";
static const std::string ATTRIBUTE_EXTEND_SORT = "sort";
static const std::string SORT_BY_WEIGHT_TAG = "sort";
static const std::string ATTRIBUTE_NAME = "attribute_name";
static const std::string PACK_ATTR_NAME_FIELD = "pack_name";
static const std::string PACK_ATTR_SUB_ATTR_FIELD = "sub_attributes";
static const std::string PACK_ATTR_COMPRESS_TYPE_FIELD = "compress_type";


static const std::string SUMMARYS = "summarys";
static const std::string SUMMARY_FIELDS = "summary_fields";
static const std::string SUMMARY_COMPRESS = "compress";
static const std::string SUMMARY_COMPRESS_BLOCK_SIZE = "compress_block_size";
static const std::string SUMMARY_COMPRESS_TYPE = "compress_type";
static const std::string SUMMARY_GROUPS = "summary_groups";
static const std::string SUMMARY_GROUP_NAME = "group_name";

static const std::string REGIONS = "regions";
static const std::string REGION_NAME = "region_name";
static const uint32_t MAX_REGION_COUNT = 1024; // 2 ^ 10

static const std::string ENABLE_TTL = "enable_ttl";
static const std::string DEFAULT_TTL = "default_ttl";
static const std::string TTL_FIELD_NAME = "ttl_field_name";
static const std::string AUTO_UPDATE_PREFERENCE = "auto_update_preference";

static const std::string SUB_SCHEMA = "sub_schema";

static const std::string TRUNCATE_PROFILES = "truncate_profiles";
static const std::string TRUNCATE_PROFILE_NAME = "truncate_profile_name";
static const std::string TRUNCATE_SORT_DESCRIPTIONS = "sort_descriptions";

static const std::string ANALYZERS = "analyzers";
static const std::string ANALYZER_NAME = "analyzer_name";
static const std::string TOKENIZER_CONFIGS = "tokenizer_configs";
static const std::string STOPWORDS = "stopwords";
static const std::string NORMALIZE_OPTIONS = "normalize_options";

static const std::string MASTER = "master";
static const std::string SLAVE = "slave";
static const std::string SCHEMA_NAME =  "schema_name";

static const std::string HIGH_FREQ_TERM_BOTH_POSTING = "both";
static const std::string HIGH_FREQ_TERM_BITMAP_POSTING = "bitmap";

static const std::string DESC_SORT_PATTERN = "DESC";
static const std::string ASC_SORT_PATTERN  = "ASC";

static const std::string DEFAULT_TRUNCATE_STRATEGY_TYPE = "default";
static const std::string TRUNCATE_META_STRATEGY_TYPE = "truncate_meta";

static const std::string INDEX_PREFERENCE_NAME = "index_preference";
static const std::string USE_NUMBER_HASH = "use_number_hash";
static const std::string PREFERENCE_TYPE_NAME = "type";
static const std::string PERF_PREFERENCE_TYPE = "PERF";
static const std::string STORE_PREFERENCE_TYPE = "STORE";
static const std::string PREFERENCE_PARAMS_NAME = "parameters";

static const std::string KKV_HASH_PARAM_NAME = "hash_dict";
static const std::string KKV_SKEY_PARAM_NAME = "suffix_key";
static const std::string SUFFIX_KEY_WRITER_LIMIT = "suffix_key_writer_limit";
static const std::string KKV_VALUE_PARAM_NAME = "value";
static const std::string KKV_VALUE_INLINE = "value_inline";

static const std::string KKV_BUILD_PROTECTION_THRESHOLD = "build_protection_threshold";

static const std::string KV_HASH_PARAM_NAME = "hash_dict";
static const std::string KV_VALUE_PARAM_NAME = "value";
static const std::string BUILD_GRANULARITY = "build_granularity";
static const std::string SEARCH_GRANULARITY = "search_granularity";


//customized config
static const std::string CUSTOMIZED_CONFIG = "customized_config";
static const std::string CUSTOMIZED_TABLE_CONFIG = "customized_table_config";
static const std::string CUSTOMIZED_PLUGIN_IDENTIFIER = "identifier";
static const std::string CUSTOMIZED_DOCUMENT_CONFIG = "customized_document_config";
static const std::string CUSTOMIZED_PLUGIN_NAME = "plugin_name";
static const std::string CUSTOMIZED_PARAMETERS = "parameters";
static const std::string CUSTOMIZED_IDENTIFIER = "id";

static const std::string CUSTOMIZED_DOCUMENT_CONFIG_RAWDOCUMENT = "document.raw_document";
static const std::string CUSTOMIZED_DOCUMENT_CONFIG_PARSER = "document.parser";

// topology string
static const std::string TOPOLOGY_SEQUENCE_STR = "sequence";
static const std::string TOPOLOGY_HASH_MODE_STR = "hash_mod";
static const std::string TOPOLOGY_HASH_RANGE_STR = "hash_range";
static const std::string TOPOLOGY_DEFAULT_STR = "default";

// recover strategy type
enum class RecoverStrategyType : int8_t
{
    RST_SEGMENT_LEVEL = 0,
    RST_VERSION_LEVEL = 1,
    RST_UNKONWN_TYPE = 127,    
};

inline std::string TopologyToStr(LevelTopology topology)
{
    switch (topology)
    {
    case topo_sequence:
        return TOPOLOGY_SEQUENCE_STR;
    case topo_hash_mod:
        return TOPOLOGY_HASH_MODE_STR;
    case topo_hash_range:
        return TOPOLOGY_HASH_RANGE_STR;
    case topo_default:
        return TOPOLOGY_DEFAULT_STR;
    }
    assert(false);
    return "unknown";
}

inline LevelTopology StrToTopology(const std::string& str)
{
    if (str == TOPOLOGY_SEQUENCE_STR)
    {
        return topo_sequence;
    }
    else if (str == TOPOLOGY_HASH_MODE_STR)
    {
        return topo_hash_mod;
    }
    else if (str == TOPOLOGY_HASH_RANGE_STR)
    {
        return topo_hash_range;
    }
    else if (str == TOPOLOGY_DEFAULT_STR)
    {
        return topo_default;
    }
    assert(false);
    return topo_default;
}

static const std::string SECTION_DIR_NAME_SUFFIX = "_section";

const size_t CACHE_DEFAULT_SIZE = 1;  // 1M
const size_t CACHE_DEFAULT_BLOCK_SIZE = 4 * 1024;  

IE_NAMESPACE_END(config);

#endif
