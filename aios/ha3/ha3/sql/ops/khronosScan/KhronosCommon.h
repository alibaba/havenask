#pragma once

#include <ha3/common.h>
#include <autil/mem_pool/Pool.h>
#include <khronos_table_interface/CommonDefine.h>

#define POOL_NEW_CLASS_ON_SHARED_PTR(pool, ptr, type, args...)          \
    (ptr).reset(POOL_NEW_CLASS((pool), type, args), [] (type *p) {      \
                POOL_DELETE_CLASS(p);                                   \
            });

BEGIN_HA3_NAMESPACE(sql);

extern const std::string KHRONOS_TIMESTAMP_TYPE;
extern const std::string KHRONOS_VALUE_TYPE;
extern const std::string KHRONOS_DATA_SERIES_VALUE_TYPE;
extern const std::string KHRONOS_WATERMARK_TYPE;

extern const std::string KHRONOS_TAGS_PREFIX;

extern const std::string KHRONOS_METRIC_TYPE;
extern const std::string KHRONOS_TAG_KEY_TYPE;
extern const std::string KHRONOS_TAG_VALUE_TYPE;
extern const std::string KHRONOS_META_TABLE_TYPE;
extern const std::string KHRONOS_DATA_POINT_TABLE_TYPE;
extern const std::string KHRONOS_DATA_SERIES_TABLE_TYPE;

extern const std::string KHRONOS_UDF_LITERAL_OR;
extern const std::string KHRONOS_UDF_ILITERAL_OR;
extern const std::string KHRONOS_UDF_NOT_LITERAL_OR;
extern const std::string KHRONOS_UDF_NOT_ILITERAL_OR;
extern const std::string KHRONOS_UDF_WILDCARD;
extern const std::string KHRONOS_UDF_IWILDCARD;
extern const std::string KHRONOS_UDF_REGEXP;

extern const std::string KHRONOS_MATCH_ALL;
extern const std::string KHRONOS_UDF_TIMESTAMP_LARGER_THAN;
extern const std::string KHRONOS_UDF_TIMESTAMP_LARGER_EQUAL_THAN;
extern const std::string KHRONOS_UDF_TIMESTAMP_LESS_THAN;
extern const std::string KHRONOS_UDF_TIMESTAMP_LESS_EQUAL_THAN;

typedef khronos::KHR_TS_TYPE KHR_TS_TYPE;
typedef khronos::KHR_WM_TYPE KHR_WM_TYPE;
typedef khronos::KHR_VALUE_TYPE KHR_VALUE_TYPE;

extern const char KHR_SERIES_KEY_METRIC_SPLITER;
extern const char KHR_SERIES_KEY_TAGS_SPLITER;
extern const char KHR_TAGKV_SPLITER;
extern const std::string KHR_TAGK_SET_SPLITER;
extern const std::string KHR_STR_SPLITER;

constexpr uint32_t KHR_DEFAULT_BUFFER_COUNT = 8 * 1024;
constexpr size_t KHR_SERIES_SCAN_DATA_POOL_LIMIT = 512 * 1024 * 1024; // 512M
constexpr size_t KHR_DEFAULT_ONE_LINE_SCAN_POINT_LIMIT = 20 * 24 * 60; // 20d && 1m
constexpr size_t KHR_COMPRESSION_BYTES_FACTOR = 10;

END_HA3_NAMESPACE(sql);
