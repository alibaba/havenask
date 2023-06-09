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
#include <assert.h>
#include <functional>
#include <limits>
#include <sstream>
#include <stdint.h>
#include <vector>

#include "autil/MultiValueType.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/MatchValue.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kv/Constant.h"
#include "indexlib/index/operation_log/Constant.h"
#include "indexlib/index/primary_key/Types.h"
#include "indexlib/index/source/Constant.h"
#include "indexlib/index/summary/Constant.h"

// TODO: rm
using namespace ::indexlib::global_enum_namespace;

// types defined in indexlibv2 code
using indexlib::attrid_t;
using indexlib::df_t;
using indexlib::dictkey_t;
using indexlib::dictvalue_t;
using indexlib::docid_t;
using indexlib::docpayload_t;
using indexlib::field_len_t;
using indexlib::fieldid_t;
using indexlib::fieldmap_t;
using indexlib::firstocc_t;
using indexlib::globalid_t;
using indexlib::indexid_t;
using indexlib::offset_t;
using indexlib::optionflag_t;
using indexlib::packattrid_t;
using indexlib::pos_t;
using indexlib::pospayload_t;
using indexlib::schema_opid_t;
using indexlib::schemaid_t;
using indexlib::section_fid_t;
using indexlib::section_len_t;
using indexlib::section_weight_t;
using indexlib::sectionid_t;
using indexlib::segmentid_t;
using indexlib::segmentindex_t;
using indexlib::short_offset_t;
using indexlib::termpayload_t;
using indexlib::tf_t;
using indexlib::ttf_t;
using indexlib::versionid_t;
using indexlib::index::format_versionid_t;
using indexlib::index::regionid_t;
using indexlib::index::summaryfieldid_t;
using indexlib::index::summarygroupid_t;

// constants defined in indexlibv2 code
using indexlib::DEFAULT_CHUNK_SIZE;
using indexlib::DEFAULT_NULL_FIELD_STRING;
using indexlib::DEFAULT_SCHEMAID;
using indexlib::DEFAULT_TIME_TO_LIVE;
using indexlib::DOC_TIME_TO_LIVE_IN_SECONDS;
using indexlib::DOCUMENT_BINARY_VERSION;
using indexlib::DOCUMENT_HASHID_TAG_KEY;
using indexlib::DOCUMENT_SOURCE_TAG_KEY;
using indexlib::EXPACK_NO_PAYLOAD;
using indexlib::EXPACK_NO_POSITION_LIST;
using indexlib::EXPACK_NO_TERM_FREQUENCY;
using indexlib::EXPACK_OPTION_FLAG_ALL;
using indexlib::HA_RESERVED_TIMESTAMP;
using indexlib::HASHMAP_INIT_SIZE;
using indexlib::INDEXLIB_MULTI_VALUE_SEPARATOR_STR;
using indexlib::INVALID_ATTRID;
using indexlib::INVALID_DICT_VALUE;
using indexlib::INVALID_DOCID;
using indexlib::INVALID_FIELDID;
using indexlib::INVALID_GLOBALID;
using indexlib::INVALID_INDEXID;
using indexlib::INVALID_PACK_ATTRID;
using indexlib::INVALID_POSITION;
using indexlib::INVALID_SCHEMA_OP_ID;
using indexlib::INVALID_SECID;
using indexlib::INVALID_SEGMENTID;
using indexlib::INVALID_TIMESTAMP;
using indexlib::MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME;
using indexlib::MAX_DOC_PER_BITMAP_BLOCK;
using indexlib::MAX_DOC_PER_BITMAP_BLOCK_BIT_NUM;
using indexlib::MAX_DOC_PER_RECORD;
using indexlib::MAX_DOC_PER_RECORD_BIT_NUM;
using indexlib::MAX_POS_PER_RECORD;
using indexlib::MAX_POS_PER_RECORD_BIT_NUM;
using indexlib::MAX_POS_PER_RECORD_MASK;
using indexlib::MULTI_VALUE_SEPARATOR;
using indexlib::NO_PAYLOAD;
using indexlib::NO_POSITION_LIST;
using indexlib::NO_TERM_FREQUENCY;
using indexlib::OPTION_FLAG_ALL;
using indexlib::OPTION_FLAG_NONE;
using indexlib::SKIP_LIST_BUFFER_SIZE;
using indexlib::SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME;
using indexlib::index::DEFAULT_MAX_OPERATION_BLOCK_SIZE;
using indexlib::index::DEFAULT_REGIONID;
using indexlib::index::DEFAULT_SUMMARYGROUPID;
using indexlib::index::DEFAULT_SUMMARYGROUPNAME;
using indexlib::index::INVALID_REGIONID;
using indexlib::index::INVALID_SUMMARYFIELDID;
using indexlib::index::INVALID_SUMMARYGROUPID;
/////////////////////////////////////////////////////

typedef indexlib::MatchValue matchvalue_t;
typedef int16_t payloadid_t;
typedef uint8_t token_len_t;
typedef int32_t interfaceid_t;
typedef uint16_t tokenattr_t;
typedef int64_t timestamp_t;
typedef uint32_t summaryid_t;
typedef uint32_t analyzerid_t;
typedef uint32_t schemavid_t;
typedef std::vector<summarygroupid_t> SummaryGroupIdVec;

constexpr size_t MAX_SPLIT_SEGMENT_COUNT = 1lu << 16;
constexpr int64_t DEFAULT_MEMORY_USE_LIMIT = (int64_t)1024 * 1024; // 1T (unit MB)
constexpr versionid_t INVALID_VERSION = -1;
// constexpr uint32_t INVALID_DOC_VERSION = -1;
constexpr docid_t UNINITIALIZED_DOCID = -2; // for expression, ha3, suez_turing
constexpr interfaceid_t DEFAULT_INTERFACE_ID = 0;
constexpr docid_t MAX_DOCID = std::numeric_limits<docid_t>::max() - 1;
// constexpr payloadid_t INVALID_PAYLOADID = (payloadid_t)-1;
// constexpr summaryid_t INVALID_SUMMARYID = (summaryid_t)-1;
// constexpr analyzerid_t INVALID_ANALYZERID = (analyzerid_t)-1;
// constexpr int32_t INVALID_INT32_ATTRIBUTE = 0xFFFFFFFE;
// constexpr uint32_t INVALID_STRUCT_PAYLOAD_LEN = 0x0000FFFF;
// constexpr uint32_t ALL_FIELDBITS = 0;
// constexpr uint32_t MAX_FIELD_BIT_COUNT = 8;
// constexpr uint32_t MAX_DOC_NUM_PER_FUXI_PARTITION = 50 * 1024 * 1024;
// constexpr uint32_t SEGMENT_DOC_COUNT_PACK = 1024;
// constexpr uint32_t SEGMENT_PACK_POWER = 10;                          // used for bit >>
// constexpr uint32_t SEGMENT_PACK_MARK = (SEGMENT_DOC_COUNT_PACK - 1); // used for bit &
// constexpr uint32_t POSTING_TABLE_SIZE_DEF = 786433;
// constexpr uint32_t TOKEN_NUM_PER_DOC_DEF = 3000;
// constexpr uint64_t CACHE_SIZE_DEF = 1 << 28; // default cache size in bytes
// constexpr double CACHE_SIZE_RATIO_DEF = 0.1; // default cache size in ratio
constexpr size_t FIELD_SORTABLE_COUNT = 17;
constexpr FieldType FIELD_SORTABLE_TYPE[FIELD_SORTABLE_COUNT] = {
    ft_integer, ft_float,  ft_long,   ft_uint32, ft_uint64, ft_hash_64, ft_hash_128, ft_int8,     ft_uint8,
    ft_int16,   ft_uint16, ft_double, ft_fp8,    ft_fp16,   ft_time,    ft_date,     ft_timestamp};

// constexpr uint64_t HASHMAP_EMPTY_KEY = std::numeric_limits<uint64_t>::max();
// constexpr uint64_t HASHMAP_EMPTY_VALUE = std::numeric_limits<uint64_t>::max();

static constexpr const char* INDEX_FORMAT_VERSION = "2.1.2";
static constexpr const char* DOC_PAYLOAD_FIELD_NAME = "DOC_PAYLOAD";
static constexpr const char* DOC_PAYLOAD_FACTOR_FIELD_NAME = "DOC_PAYLOAD_FACTOR";
static constexpr const char* DOC_PAYLOAD_USE_FP_16 = "DOC_PAYLOAD_USE_FP_16";
static constexpr const char* BITMAP_TRUNCATE_INDEX_NAME = "BITMAP"; // for kiwi, ha3
static constexpr const char* INDEXLIB_BUILD_INFO_COLLECTOR_NAME = "index_build_info";

inline const std::string DEFAULT_HASH_ID_FIELD_NAME = "_doc_hash_id_";
// inline const std::string DOCUMENT_HASHFIELD_TAG_KEY = "__hash_field__";
// inline const std::string DOCUMENT_INGESTIONTIMESTAMP_TAG_KEY = "__ingestion_timestamp__";
inline const std::string DEFAULT_REGIONNAME = "__DEFAULT_INDEX_REGION__";

// enum ConditionFilterValueType {
//     cfvt_signed,
//     cfvt_unsigned,
//     cfvt_float,
// };
// enum SupportLanguage { sl_English, sl_Chinese, sl_Traditional, sl_Japanese, sl_Korean, sl_unknown };
// enum SupportCoding { sc_UTF8, sc_Unicode, sc_Big5, sc_GBK, sc_Ansi };
// enum IndexExtend {
//     ie_none = 0,
//     ie_occ_none = 1,
//     ie_occ_array = 2,
//     ie_occ_first = 4,
//     ie_int = 8,
//     ie_pk = 16,
//     ie_md5 = 32,
//     ie_double = 64,
//     ie_range = 128,
//     ie_wildcard = 256,
//     ie_cache = 512,
//     ie_stopword_ignore = 1024,
// };
enum SegmentType {
    st_normal,
    st_merge,
};

// TODO: fix the warning of "var unused" in indexlib.h
// static inline void __FOR_UNUSED_VAR_FUNC__()
// {
//     (void)MAX_DOCID;
//     (void)INVALID_POSITION;
// }

//////////////////////////////////////////////////////////////////////
// indexlib namespace
namespace indexlib {

constexpr FieldType VIRTUAL_TIMESTAMP_FIELD_TYPE = ft_int64;
constexpr InvertedIndexType VIRTUAL_TIMESTAMP_INDEX_TYPE = it_number_int64;
inline const std::string EMPTY_STRING = "";
inline const std::string TIMESTAMP_FIELD = "timestamp_field";

enum TableType {
    tt_index,
    tt_kv,
    tt_kkv,
    tt_rawfile,
    tt_linedata,
    tt_customized,
    tt_orc,
};

struct PartitionRange {
    uint32_t from = 0;
    uint32_t to = 0;

    PartitionRange(uint32_t f, uint32_t t) : from(f), to(t) {}
    PartitionRange() = default;
    bool IsValid() { return from != to; }
    bool operator==(const PartitionRange& other) const { return from == other.from && to == other.to; }
};

using namespace ::indexlib::enum_namespace;
} // namespace indexlib
