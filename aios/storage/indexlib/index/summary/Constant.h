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

#include "indexlib/base/Constant.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/summary/Types.h"

namespace indexlib::index {

static constexpr summaryfieldid_t INVALID_SUMMARYFIELDID = (summaryfieldid_t)-1;
static constexpr summarygroupid_t INVALID_SUMMARYGROUPID = (summarygroupid_t)-1;
static constexpr summarygroupid_t DEFAULT_SUMMARYGROUPID = 0;
inline const std::string SUMMARY_OFFSET_FILE_NAME = "offset";
inline const std::string SUMMARY_DATA_FILE_NAME = "data";
inline const std::string DEFAULT_SUMMARYGROUPNAME = "__DEFAULT_SUMMARYGROUPNAME__";

inline const std::string SUMMARY_FIELDS = "summary_fields";
inline const std::string SUMMARY_GROUP_PARAMTETER = "parameter";
inline const std::string SUMMARY_ADAPTIVE_OFFSET = "adaptive_offset";
inline const std::string SUMMARY_GROUPS = "summary_groups";
inline const std::string SUMMARY_GROUP_NAME = "group_name";

} // namespace indexlib::index

// TODO: rm
namespace indexlibv2::index {
using indexlib::index::DEFAULT_SUMMARYGROUPID;
using indexlib::index::DEFAULT_SUMMARYGROUPNAME;
using indexlib::index::INVALID_SUMMARYFIELDID;
using indexlib::index::INVALID_SUMMARYGROUPID;
using indexlib::index::SUMMARY_DATA_FILE_NAME;
using indexlib::index::SUMMARY_OFFSET_FILE_NAME;

} // namespace indexlibv2::index
