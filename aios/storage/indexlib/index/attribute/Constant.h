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

#include <string>

#include "indexlib/index/attribute/Types.h"

namespace indexlib::index {
constexpr uint32_t ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT = 50;
constexpr uint32_t ATTRIBUTE_U32OFFSET_THRESHOLD_MAX = 0xFFFFFFFFL;

inline const std::string ATTRIBUTE_DATA_FILE_NAME = "data";
inline const std::string ATTRIBUTE_OFFSET_FILE_NAME = "offset";
inline const std::string ATTRIBUTE_DATA_INFO_FILE_NAME = "data_info";
inline const std::string ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME = "extend_slice_data";
inline const std::string ATTRIBUTE_OFFSET_EXTEND_SUFFIX = ".extend64";
inline const std::string ATTRIBUTE_EQUAL_COMPRESS_UPDATE_EXTEND_SUFFIX = ".extend_equal_compress";
inline const std::string ATTRIBUTE_UPDATABLE = "updatable";
inline const std::string ATTRIBUTE_DEFRAG_SLICE_PERCENT = "defrag_slice_percent";
inline const std::string ATTRIBUTE_U32OFFSET_THRESHOLD = "u32offset_threshold";
inline const std::string ATTRIBUTE_COMPRESS_TYPE = "compress_type";
inline const std::string ATTRIBUTE_SLICE_COUNT = "slice_count";
inline const std::string ATTRIBUTE_SLICE_IDX = "slice_idx";
} // namespace indexlib::index

namespace indexlib {
constexpr attrid_t INVALID_ATTRID = (attrid_t)-1;
constexpr packattrid_t INVALID_PACK_ATTRID = (packattrid_t)-1;
} // namespace indexlib

//////////////////////////////////////////////////////////////////////
// TODO: rm
namespace indexlibv2::index {
using indexlib::index::ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME;
using indexlib::index::ATTRIBUTE_DATA_FILE_NAME;
using indexlib::index::ATTRIBUTE_DATA_INFO_FILE_NAME;
using indexlib::index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT;
using indexlib::index::ATTRIBUTE_DEFRAG_SLICE_PERCENT;
using indexlib::index::ATTRIBUTE_EQUAL_COMPRESS_UPDATE_EXTEND_SUFFIX;
using indexlib::index::ATTRIBUTE_OFFSET_EXTEND_SUFFIX;
using indexlib::index::ATTRIBUTE_OFFSET_FILE_NAME;
using indexlib::index::ATTRIBUTE_SLICE_COUNT;
using indexlib::index::ATTRIBUTE_SLICE_IDX;
using indexlib::index::ATTRIBUTE_U32OFFSET_THRESHOLD;
using indexlib::index::ATTRIBUTE_U32OFFSET_THRESHOLD_MAX;
using indexlib::index::ATTRIBUTE_UPDATABLE;
} // namespace indexlibv2::index

namespace indexlibv2 {
using indexlib::INVALID_ATTRID;
using indexlib::INVALID_PACK_ATTRID;
} // namespace indexlibv2
