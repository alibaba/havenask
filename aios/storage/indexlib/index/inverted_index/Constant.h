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

#include "indexlib/base/Types.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlib {
constexpr sectionid_t INVALID_SECID = (sectionid_t)-1;
constexpr docid_t END_DOCID = std::numeric_limits<docid_t>::max();
constexpr pospayload_t INVALID_POS_PAYLOAD = 0;
constexpr docpayload_t INVALID_DOC_PAYLOAD = 0;
constexpr tf_t INVALID_TF = -1;
constexpr ttf_t INVALID_TTF = -1;
constexpr df_t INVALID_DF = -1;
constexpr pos_t INVALID_POSITION = std::numeric_limits<pos_t>::max();

constexpr optionflag_t OPTION_FLAG_ALL =
    of_term_payload | of_doc_payload | of_position_payload | of_position_list | of_term_frequency;
constexpr optionflag_t EXPACK_OPTION_FLAG_ALL = OPTION_FLAG_ALL | of_fieldmap;
constexpr optionflag_t OPTION_FLAG_NONE = of_none;
constexpr optionflag_t NO_PAYLOAD = of_position_list | of_term_frequency;
constexpr optionflag_t NO_POSITION_LIST = of_doc_payload | of_term_payload | of_term_frequency;
constexpr optionflag_t NO_TERM_FREQUENCY = of_doc_payload | of_term_payload;
constexpr optionflag_t EXPACK_NO_PAYLOAD = of_position_list | of_term_frequency | of_fieldmap;
constexpr optionflag_t EXPACK_NO_POSITION_LIST = of_doc_payload | of_term_payload | of_term_frequency | of_fieldmap;
constexpr optionflag_t EXPACK_NO_TERM_FREQUENCY = of_doc_payload | of_term_payload | of_fieldmap;

constexpr uint32_t MAX_DOC_PER_RECORD = 128;
constexpr uint32_t MAX_DOC_PER_RECORD_BIT_NUM = 7;
constexpr uint32_t MAX_POS_PER_RECORD = 128;
constexpr uint32_t MAX_POS_PER_RECORD_BIT_NUM = 7;
constexpr uint32_t MAX_POS_PER_RECORD_MASK = MAX_POS_PER_RECORD - 1;
constexpr uint32_t MAX_DOC_PER_BITMAP_BLOCK = 256;
constexpr uint32_t MAX_DOC_PER_BITMAP_BLOCK_BIT_NUM = 8;
constexpr uint8_t MAX_DICT_INLINE_AVAILABLE_SIZE = 7;
constexpr uint8_t COMPRESS_MODE_SIZE = 4;
constexpr uint32_t MAX_UNCOMPRESSED_SKIP_LIST_SIZE = 10;
constexpr uint8_t SKIP_LIST_BUFFER_SIZE = 32;

static constexpr const char* TRUNCATE_ATTRIBUTE_READER_CREATOR = "truncate_attribute_reader_creator";
static constexpr const char* TRUNCATE_META_FILE_READER_CREATOR = "truncate_meta_file_reader_creator";
static constexpr const char* TRUNCATE_OPTION_CONFIG = "truncate_option_config";
static constexpr const char* TRUNCATE_META_STRATEGY_TYPE = "truncate_meta";
static constexpr const char* SEGMENT_MERGE_PLAN_INDEX = "segment_merge_plan_index";
static constexpr const char* SOURCE_VERSION_TIMESTAMP = "source_version_timestamp"; // second

static constexpr const char* DOC_PAYLOAD_FIELD_NAME = "DOC_PAYLOAD";
static constexpr const char* DOC_PAYLOAD_FACTOR_FIELD_NAME = "DOC_PAYLOAD_FACTOR";
static constexpr const char* DOC_PAYLOAD_USE_FP_16 = "DOC_PAYLOAD_USE_FP_16";

} // namespace indexlib

namespace indexlib::index {
constexpr uint32_t MAX_UNCOMPRESSED_DOC_LIST_SIZE = 5;
constexpr uint32_t MAX_UNCOMPRESSED_POS_LIST_SIZE = 5;
constexpr uint8_t BIG_ENDIAN_DICT_INLINE_SHIFT_BITNUM = 8;
inline const std::string INDEX_FORMAT_OPTION_FILE_NAME = "index_format_option";
inline const std::string DICTIONARY_FILE_NAME = "dictionary";
inline const std::string POSTING_FILE_NAME = "posting";
inline const std::string BITMAP_DICTIONARY_FILE_NAME = "bitmap_dictionary";
inline const std::string BITMAP_POSTING_FILE_NAME = "bitmap_posting";
inline const std::string BITMAP_POSTING_EXPAND_FILE_NAME = "bitmap_posting_expand";

} // namespace indexlib::index

// TODO: rm
namespace indexlibv2 {
using indexlib::EXPACK_NO_PAYLOAD;
using indexlib::EXPACK_NO_POSITION_LIST;
using indexlib::EXPACK_NO_TERM_FREQUENCY;
using indexlib::EXPACK_OPTION_FLAG_ALL;
using indexlib::INVALID_POSITION;
using indexlib::INVALID_SECID;
using indexlib::MAX_DICT_INLINE_AVAILABLE_SIZE;
using indexlib::MAX_DOC_PER_BITMAP_BLOCK;
using indexlib::MAX_DOC_PER_BITMAP_BLOCK_BIT_NUM;
using indexlib::MAX_DOC_PER_RECORD;
using indexlib::MAX_DOC_PER_RECORD_BIT_NUM;
using indexlib::MAX_POS_PER_RECORD;
using indexlib::MAX_POS_PER_RECORD_BIT_NUM;
using indexlib::MAX_POS_PER_RECORD_MASK;
using indexlib::NO_PAYLOAD;
using indexlib::NO_POSITION_LIST;
using indexlib::NO_TERM_FREQUENCY;
using indexlib::OPTION_FLAG_ALL;
using indexlib::OPTION_FLAG_NONE;
} // namespace indexlibv2

namespace indexlibv2::index {
using indexlib::index::INDEX_FORMAT_OPTION_FILE_NAME;
} // namespace indexlibv2::index
