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

#include "indexlib/base/Types.h"
#include "indexlib/index/common/Types.h"

namespace indexlib {
constexpr indexid_t INVALID_INDEXID = std::numeric_limits<indexid_t>::max();
constexpr size_t INVALID_SHARDID = std::numeric_limits<size_t>::max();
constexpr dictvalue_t INVALID_DICT_VALUE = (dictvalue_t)-1;
static constexpr const char* MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME = "main_docid_to_sub_docid_attr_name";
static constexpr const char* SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME = "sub_docid_to_main_docid_attr_name";
} // namespace indexlib

namespace indexlib::index {
constexpr size_t VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD = 0xFFFFFFFFL;
constexpr size_t EQUAL_COMPRESS_UPDATE_SLICE_LEN = 64 * 1024 * 1024;
constexpr size_t EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT = 1024;
// constexpr uint32_t SECTION_TAIL_MAGIC = 0xF1F2F3F4;
constexpr uint32_t UINT32_OFFSET_TAIL_MAGIC = 0xF2F3F4F5;
constexpr uint32_t UINT64_OFFSET_TAIL_MAGIC = 0xF3F4F5F6;
inline const std::string COMPRESS_TYPE = "compress_type";
inline const std::string FILE_COMPRESSOR = "file_compressor";
inline const std::string INDEX_DIR_NAME = "index";
inline const std::string ATTRIBUTE_DIR_NAME = "attribute";
inline const std::string GENERAL_VALUE_INDEX_TYPE_STR = "__value__";
} // namespace indexlib::index

//////////////////////////////////////////////////////////////////////
namespace indexlibv2 {
using indexlib::INVALID_INDEXID;
} // namespace indexlibv2

namespace indexlibv2::index {
using indexlib::index::ATTRIBUTE_DIR_NAME;
using indexlib::index::COMPRESS_TYPE;
using indexlib::index::EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT;
using indexlib::index::EQUAL_COMPRESS_UPDATE_SLICE_LEN;
using indexlib::index::FILE_COMPRESSOR;
using indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR;
using indexlib::index::INDEX_DIR_NAME;
using indexlib::index::UINT32_OFFSET_TAIL_MAGIC;
using indexlib::index::UINT64_OFFSET_TAIL_MAGIC;
using indexlib::index::VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD;
} // namespace indexlibv2::index
