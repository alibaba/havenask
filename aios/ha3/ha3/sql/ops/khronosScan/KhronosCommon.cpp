#include <ha3/sql/ops/khronosScan/KhronosCommon.h>

using namespace std;

BEGIN_HA3_NAMESPACE(sql);

const string KHRONOS_TIMESTAMP_TYPE = "khronos_timestamp";
const string KHRONOS_VALUE_TYPE = "khronos_value";
const string KHRONOS_DATA_SERIES_VALUE_TYPE = "khronos_series";

const string KHRONOS_TAGS_PREFIX = "$tags";

const string KHRONOS_METRIC_TYPE = "khronos_metric";
const string KHRONOS_TAG_KEY_TYPE = "khronos_tag_key";
const string KHRONOS_TAG_VALUE_TYPE = "khronos_tag_value";
const string KHRONOS_META_TABLE_TYPE = "khronos_meta";
const string KHRONOS_DATA_POINT_TABLE_TYPE = "khronos_data";
const string KHRONOS_DATA_SERIES_TABLE_TYPE = "khronos_series_data";
const string KHRONOS_WATERMARK_TYPE = "khronos_watermark";

const string KHRONOS_UDF_LITERAL_OR = "literal_or";
const string KHRONOS_UDF_ILITERAL_OR = "iliteral_or";
const string KHRONOS_UDF_NOT_LITERAL_OR = "not_literal_or";
const string KHRONOS_UDF_NOT_ILITERAL_OR = "not_iliteral_or";
const string KHRONOS_UDF_WILDCARD = "wildcard";
const string KHRONOS_UDF_IWILDCARD = "iwildcard";
const string KHRONOS_UDF_REGEXP = "tsdb_regexp";
const string KHRONOS_MATCH_ALL = "*";
const string KHRONOS_UDF_TIMESTAMP_LARGER_THAN = "timestamp_larger_than";
const string KHRONOS_UDF_TIMESTAMP_LARGER_EQUAL_THAN = "timestamp_larger_equal_than";
const string KHRONOS_UDF_TIMESTAMP_LESS_THAN = "timestamp_less_than";
const string KHRONOS_UDF_TIMESTAMP_LESS_EQUAL_THAN = "timestamp_less_equal_than";
const char KHR_SERIES_KEY_METRIC_SPLITER = '\x1E';
const char KHR_SERIES_KEY_TAGS_SPLITER = '\x1D';
const char KHR_TAGKV_SPLITER = '\x1F';

// const string KHR_TAGK_SET_SPLITER = ",";
// const string KHR_STR_SPLITER = ":";

END_HA3_NAMESPACE(sql);
