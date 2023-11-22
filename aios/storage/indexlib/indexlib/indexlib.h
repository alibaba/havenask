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

#include "indexlib/legacy/indexlib.h"

// TODO: rm
using namespace ::indexlib::global_enum_namespace;
// TODO: rm these using
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
using indexlib::regionid_t;
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
using indexlib::index::summaryfieldid_t;
using indexlib::index::summarygroupid_t;
using indexlib::index::token_len_t;
// constants defined in indexlibv2 code
using indexlib::DEFAULT_CHUNK_SIZE;
using indexlib::DEFAULT_NULL_FIELD_STRING;
using indexlib::DEFAULT_REGIONID;
using indexlib::DEFAULT_SCHEMAID;
using indexlib::DEFAULT_TIME_TO_LIVE;
using indexlib::DOC_PAYLOAD_FACTOR_FIELD_NAME;
using indexlib::DOC_PAYLOAD_FIELD_NAME;
using indexlib::DOC_PAYLOAD_USE_FP_16;
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
using indexlib::INDEX_FORMAT_VERSION;
using indexlib::INDEXLIB_MULTI_VALUE_SEPARATOR_STR;
using indexlib::INVALID_ATTRID;
using indexlib::INVALID_DICT_VALUE;
using indexlib::INVALID_DOCID;
using indexlib::INVALID_FIELDID;
using indexlib::INVALID_GLOBALID;
using indexlib::INVALID_INDEXID;
using indexlib::INVALID_PACK_ATTRID;
using indexlib::INVALID_POSITION;
using indexlib::INVALID_REGIONID;
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
using indexlib::MAX_SPLIT_SEGMENT_COUNT;
using indexlib::MULTI_VALUE_SEPARATOR;
using indexlib::NO_PAYLOAD;
using indexlib::NO_POSITION_LIST;
using indexlib::NO_TERM_FREQUENCY;
using indexlib::OPTION_FLAG_ALL;
using indexlib::OPTION_FLAG_NONE;
using indexlib::SKIP_LIST_BUFFER_SIZE;
using indexlib::SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME;
using indexlib::index::DEFAULT_MAX_OPERATION_BLOCK_SIZE;
using indexlib::index::DEFAULT_SUMMARYGROUPID;
using indexlib::index::DEFAULT_SUMMARYGROUPNAME;
using indexlib::index::INVALID_SUMMARYFIELDID;
using indexlib::index::INVALID_SUMMARYGROUPID;
//////////////////////////////////////////////////////////////////////
// indexlib namespace
namespace indexlib {
typedef int64_t timestamp_t;
static constexpr const char* INDEXLIB_BUILD_INFO_COLLECTOR_NAME = "index_build_info";
static constexpr const char* BITMAP_TRUNCATE_INDEX_NAME = "BITMAP"; // for kiwi, ha3
inline const std::string TIMESTAMP_FIELD = "timestamp_field";
constexpr docid_t MAX_DOCID = std::numeric_limits<docid_t>::max() - 1;
constexpr docid_t UNINITIALIZED_DOCID = -2; // for expression, ha3, suez_turing

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
