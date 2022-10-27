#ifndef ISEARCH_HA3_COMMON_DEF_H
#define ISEARCH_HA3_COMMON_DEF_H

#include <ha3/isearch.h>
#include <suez/turing/expression/common.h>

BEGIN_HA3_NAMESPACE(common);
// // serialize level
// static const uint8_t HA3_SL_MAX = 255;
// static const uint8_t HA3_SL_VARIABLE = 40;        // serialize value for frontend/sp/cache/proxy/qrs
// static const uint8_t HA3_SL_ATTRIBUTE = 30;       // serialize value for sp/cache/proxy/qrs
// static const uint8_t HA3_SL_CACHE = 20;           // serialize value for cache/proxy/qrs
// static const uint8_t HA3_SL_QRS = 10; // serialize value for proxy/qrs
// static const uint8_t HA3_SL_NONE = 0;            // not serialize

// static const std::string RAW_PRIMARYKEY_REF = BUILD_IN_REFERENCE_PREFIX"rawpk";
// static const std::string PRIMARYKEY_REF = BUILD_IN_REFERENCE_PREFIX"pkattr";
static const std::string INDEXVERSION_REF = suez::turing::BUILD_IN_REFERENCE_PREFIX + "indexversion";
static const std::string FULLVERSION_REF = suez::turing::BUILD_IN_REFERENCE_PREFIX + "fullversion";
static const std::string IP_REF = suez::turing::BUILD_IN_REFERENCE_PREFIX + "ip";

static const std::string IS_EVALUATED_REFERENCE = suez::turing::BUILD_IN_REFERENCE_PREFIX + "is_evaluated_reference";
static const std::string HASH_ID_REF = suez::turing::BUILD_IN_REFERENCE_PREFIX + "hashid";
static const std::string SUB_HASH_ID_REF = suez::turing::BUILD_IN_REFERENCE_PREFIX + "sub_hashid";
static const std::string CLUSTER_ID_REF = suez::turing::BUILD_IN_REFERENCE_PREFIX + "clusterid";
static const std::string MATCH_DATA_REF = suez::turing::BUILD_IN_REFERENCE_PREFIX + "match_data";
static const std::string SIMPLE_MATCH_DATA_REF = suez::turing::BUILD_IN_REFERENCE_PREFIX + "simple_match_data";
static const std::string SUB_SIMPLE_MATCH_DATA_REF = suez::turing::BUILD_IN_REFERENCE_PREFIX + "sub_simple_match_data";
static const std::string MATCH_VALUE_REF = suez::turing::BUILD_IN_REFERENCE_PREFIX + "match_value";
static const std::string DISTINCT_INFO_REF = suez::turing::BUILD_IN_REFERENCE_PREFIX + "distinct_info";

static const std::string SUB_DOC_REF_PREFIX = "_@_sub_doc_";

static const std::string GROUP_KEY_REF = "group_key";

static const std::string HA3_DEFAULT_GROUP = "ha3_default_group";
static const std::string HA3_GLOBAL_INFO_GROUP = "ha3_global_info_group";
static const std::string CLUSTER_ID_REF_GROUP = suez::turing::BUILD_IN_REFERENCE_PREFIX + "clusterid_group";
static const std::string HA3_MATCHVALUE_GROUP = "ha3_matchvalue_group";
static const std::string HA3_MATCHDATA_GROUP = "ha3_matchdata_group";
static const std::string HA3_SUBMATCHDATA_GROUP = "ha3_submatchdata_group";
static const std::string HA3_RESULT_TENSOR_NAME = "ha3_result";

static const std::string OP_DEBUG_KEY = "op_debug";


END_HA3_NAMESPACE(common);

#endif //ISEARCH_HA3_COMMON_DEF_H
