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
#pragma once

#include <limits>
#include <string>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

extern const std::string TABLE_NAME;
extern const std::string TABLE_TYPE;
extern const std::string TABLE_TYPE_INDEX;
extern const std::string TABLE_TYPE_LINEDATA;
extern const std::string TABLE_TYPE_RAWFILE;
extern const std::string TABLE_TYPE_KV;
extern const std::string TABLE_TYPE_KKV;
extern const std::string TABLE_TYPE_CUSTOMIZED;
extern const std::string TABLE_TYPE_ORC;
extern const std::string SCHEMA_VERSION_ID;
extern const std::string MAX_SCHEMA_MODIFY_OPERATION_COUNT;
extern const std::string SCHEMA_MODIFY_OPERATIONS;
extern const std::string SCHEMA_MODIFY_ADD;
extern const std::string SCHEMA_MODIFY_DEL;
extern const std::string SCHEMA_MODIFY_PARAMETER;

extern const std::string TABLE_USER_DEFINED_PARAM;
extern const std::string GLOBAL_REGION_INDEX_PREFERENCE;

extern const std::string TABLES;

extern const std::string DICTIONARIES;
extern const std::string DICTIONARY_NAME;
extern const std::string DICTIONARY_CONTENT;

extern const std::string ADAPTIVE_DICTIONARIES;
extern const std::string ADAPTIVE_DICTIONARY_NAME;
extern const std::string ADAPTIVE_DICTIONARY_TYPE;
extern const std::string ADAPTIVE_DICTIONARY_THRESHOLD;
constexpr uint32_t INVALID_ADAPTIVE_THRESHOLD = -1;

extern const std::string FIELDS;
extern const std::string FIELD_NAME;
extern const std::string FIELD_TYPE;
extern const std::string FIELD_VALID_VALUES;
extern const std::string FIELD_MULTI_VALUE;
extern const std::string FIELD_ANALYZER;
extern const std::string ATTRIBUTE_UNIQ_ENCODE;
extern const std::string ATTRIBUTE_UPDATABLE_MULTI_VALUE;
extern const std::string FIELD_BINARY;
extern const std::string FIELD_USER_DEFINED_PARAM;
// extern const std::string ATTRIBUTE_U32OFFSET_THRESHOLD;
extern const std::string FIELD_VIRTUAL;
extern const std::string FIELD_DEFAULT_VALUE;
extern const std::string FIELD_DEFAULT_STR_VALUE;
extern const std::string FIELD_SEPARATOR;

extern const std::string FIELD_SUPPORT_NULL;
extern const std::string FIELD_IS_BUILT_IN;
extern const std::string FIELD_DEFAULT_NULL_STRING_VALUE;
extern const std::string FIELD_USER_DEFINE_ATTRIBUTE_NULL_VALUE;

extern const std::string FIELD_DEFAULT_TIME_ZONE;

// extern const std::string COMPRESS_TYPE;
extern const std::string COMPRESS_UNIQ;
extern const std::string COMPRESS_EQUAL;
extern const std::string COMPRESS_BLOCKFP;
extern const std::string COMPRESS_FP16;
extern const std::string COMPRESS_PATCH;
extern const std::string COMPRESS_INT8_PREFIX;
// extern const std::string FILE_COMPRESS;
// extern const std::string FILE_COMPRESS_SCHEMA;

extern const std::string PAYLOAD_NAME;
extern const std::string PAYLOAD_ATOMIC_UPDATE;
extern const std::string PAYLOAD_FIELDS;

extern const std::string SOURCE;
extern const std::string INDEXS;
extern const std::string INDEX_NAME;
extern const std::string INDEX_ANALYZER;
extern const std::string INDEX_TYPE;
extern const std::string INDEX_FORMAT_VERSIONID;
extern const std::string INDEX_FIELDS;
extern const std::string FIELD_BOOST;
extern const std::string NEED_SHARDING;
extern const std::string SHARDING_COUNT;
extern const std::string TERM_PAYLOAD_FLAG;
extern const std::string DOC_PAYLOAD_FLAG;
extern const std::string POSITION_PAYLOAD_FLAG;
extern const std::string POSITION_LIST_FLAG;
extern const std::string TERM_FREQUENCY_FLAG;
extern const std::string TERM_FREQUENCY_BITMAP;
extern const std::string HAS_PRIMARY_KEY_ATTRIBUTE;
extern const std::string HIGH_FEQUENCY_DICTIONARY;
extern const std::string HIGH_FEQUENCY_ADAPTIVE_DICTIONARY;
extern const std::string HIGH_FEQUENCY_TERM_POSTING_TYPE;
extern const std::string HAS_SECTION_ATTRIBUTE;
extern const std::string SECTION_ATTRIBUTE_CONFIG;
extern const std::string HAS_SECTION_WEIGHT;
extern const std::string HAS_SECTION_FIELD_ID;
extern const std::string MAX_FIRSTOCC_IN_DOC;
extern const std::string HIGH_FREQUENCY_DICTIONARY_SELF_ADAPTIVE_FLAG;
extern const std::string HAS_TRUNCATE;
extern const std::string HAS_SHORTLIST_VBYTE_COMPRESS;
extern const std::string INDEX_COMPRESS_MODE;
extern const std::string INDEX_COMPRESS_MODE_PFOR_DELTA;
extern const std::string INDEX_COMPRESS_MODE_REFERENCE;
extern const std::string USE_TRUNCATE_PROFILES;
extern const std::string USE_TRUNCATE_PROFILES_SEPRATOR;
extern const std::string USE_HASH_DICTIONARY;
extern const std::string TRUNCATE_INDEX_NAME_MAPPER;
extern const std::string USE_NUMBER_PK_HASH;
extern const std::string PRIMARY_KEY_STORAGE_TYPE;
extern const std::string INDEX_UPDATABLE;
extern const std::string PATCH_COMPRESSED;
extern const std::string SORT_FIELD;

// customize index
extern const std::string CUSTOM_DATA_DIR_NAME;
extern const std::string CUSTOMIZED_INDEXER_NAME;
extern const std::string CUSTOMIZED_INDEXER_PARAMS;

// spatial index
extern const std::string MAX_SEARCH_DIST;
extern const std::string MAX_DIST_ERROR;
extern const std::string DIST_LOSS_ACCURACY;
extern const std::string DIST_CALCULATOR;

extern const std::string CALCULATOR_HAVERSINE;

extern const std::string ATTRIBUTES;
extern const std::string ATTRIBUTE_EXTEND;
extern const std::string ATTRIBUTE_EXTEND_SORT;
extern const std::string SORT_BY_WEIGHT_TAG;
extern const std::string ATTRIBUTE_NAME;
// extern const std::string PACK_ATTR_NAME_FIELD;
// extern const std::string PACK_ATTR_SUB_ATTR_FIELD;
// extern const std::string PACK_ATTR_COMPRESS_TYPE_FIELD;
// extern const std::string PACK_ATTR_VALUE_FORMAT;
// extern const std::string PACK_ATTR_VALUE_FORMAT_IMPACT;
// extern const std::string PACK_ATTR_VALUE_FORMAT_PLAIN;

extern const std::string SUMMARYS;
extern const std::string SUMMARY_FIELDS;
extern const std::string SUMMARY_COMPRESS;
extern const std::string SUMMARY_COMPRESS_BLOCK_SIZE;
// extern const std::string SUMMARY_COMPRESS_TYPE;
extern const std::string SUMMARY_GROUPS;
extern const std::string SUMMARY_GROUP_NAME;
extern const std::string SUMMARY_GROUP_PARAMTETER;
extern const std::string SUMMARY_ADAPTIVE_OFFSET;

extern const std::string REGIONS;
extern const std::string REGION_NAME;
constexpr uint32_t MAX_REGION_COUNT = 1024; // 2 ^ 10

extern const std::string ENABLE_TTL;
extern const std::string DEFAULT_TTL;
extern const std::string TTL_FIELD_NAME;
extern const std::string TTL_FROM_DOC;
extern const std::string AUTO_UPDATE_PREFERENCE;
extern const std::string INSERT_OR_IGNORE;

extern const std::string ENABLE_TEMPERATURE_LAYER;
extern const std::string TEMPERATURE_LAYER_CONFIG;

extern const std::string ENABLE_HASH_ID;
extern const std::string HASH_ID_FIELD_NAME;

extern const std::string ORDER_PRESERVING_FIELD;

extern const std::string SUB_SCHEMA;

extern const std::string TRUNCATE_PROFILES;
extern const std::string TRUNCATE_PROFILE_NAME;
extern const std::string TRUNCATE_SORT_DESCRIPTIONS;
extern const std::string TRUNCATE_DOC_PAYLOAD_PARAMETERS;
extern const std::string TRUNCATE_PAYLOAD_NAME;

extern const std::string ANALYZERS;
extern const std::string ANALYZER_NAME;
extern const std::string TOKENIZER_CONFIGS;
extern const std::string STOPWORDS;
extern const std::string NORMALIZE_OPTIONS;

extern const std::string MASTER;
extern const std::string SLAVE;
extern const std::string SCHEMA_NAME;

extern const std::string HIGH_FREQ_TERM_BOTH_POSTING;
extern const std::string HIGH_FREQ_TERM_BITMAP_POSTING;

extern const std::string DESC_SORT_PATTERN;
extern const std::string ASC_SORT_PATTERN;

extern const std::string DEFAULT_TRUNCATE_STRATEGY_TYPE;
extern const std::string TRUNCATE_META_STRATEGY_TYPE;

extern const std::string INDEX_PREFERENCE_NAME;
extern const std::string USE_NUMBER_HASH;
extern const std::string PREFERENCE_TYPE_NAME;
extern const std::string PERF_PREFERENCE_TYPE;
extern const std::string STORE_PREFERENCE_TYPE;
extern const std::string PREFERENCE_PARAMS_NAME;

extern const std::string KKV_HASH_PARAM_NAME;
extern const std::string KKV_SKEY_PARAM_NAME;
extern const std::string SUFFIX_KEY_WRITER_LIMIT;
extern const std::string KKV_VALUE_PARAM_NAME;
extern const std::string KKV_VALUE_INLINE;

extern const std::string KKV_BUILD_PROTECTION_THRESHOLD;

extern const std::string KV_HASH_PARAM_NAME;
extern const std::string KV_VALUE_PARAM_NAME;
extern const std::string BUILD_GRANULARITY;
extern const std::string SEARCH_GRANULARITY;

// customized config
extern const std::string CUSTOMIZED_CONFIG;
extern const std::string CUSTOMIZED_TABLE_CONFIG;
extern const std::string CUSTOMIZED_PLUGIN_IDENTIFIER;
extern const std::string CUSTOMIZED_DOCUMENT_CONFIG;
extern const std::string CUSTOMIZED_PLUGIN_NAME;
extern const std::string CUSTOMIZED_PARAMETERS;
extern const std::string CUSTOMIZED_IDENTIFIER;

extern const std::string CUSTOMIZED_DOCUMENT_CONFIG_RAWDOCUMENT;
extern const std::string CUSTOMIZED_DOCUMENT_CONFIG_PARSER;

// topology string
extern const std::string TOPOLOGY_SEQUENCE_STR;
extern const std::string TOPOLOGY_HASH_MODE_STR;
extern const std::string TOPOLOGY_KEY_RANGE_STR;
extern const std::string TOPOLOGY_DEFAULT_STR;

// recover strategy type
enum class RecoverStrategyType : int8_t {
    RST_SEGMENT_LEVEL = 0,
    RST_VERSION_LEVEL = 1,
    RST_UNKONWN_TYPE = 127,
};

enum ReadPreference {
    RP_RANDOM_PREFER,
    RP_SEQUENCE_PREFER,
};

extern const std::string SECTION_DIR_NAME_SUFFIX;

constexpr size_t CACHE_DEFAULT_SIZE = 1; // 1M
constexpr size_t CACHE_DEFAULT_BLOCK_SIZE = 4 * 1024;
constexpr size_t CACHE_DEFAULT_IO_BATCH_SIZE = 4;

// source config
extern const std::string SOURCE_GROUP_CONFIG_FIELD_MODE;
extern const std::string SOURCE_GROUP_CONFIG_FIELDS;
extern const std::string SOURCE_GROUP_CONFIG_PARAMETER;
extern const std::string SOURCE_GROUP_CONFIG_MODULE;
extern const std::string SOURCE_GROUP_CONFIG_MODULE_PARAMS;

extern const std::string SOURCE_CONFIG_MODULES;
extern const std::string SOURCE_GROUP_CONFIGS;

extern const std::string TABLET;
}} // namespace indexlib::config
