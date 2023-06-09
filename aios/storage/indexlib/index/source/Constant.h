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

#include "indexlib/index/source/Types.h"

namespace indexlib::index {
static constexpr const char SOURCE_DATA_DIR_PREFIX[] = "group";
static constexpr const char SOURCE_DATA_FILE_NAME[] = "data";
static constexpr const char SOURCE_OFFSET_FILE_NAME[] = "offset";
static constexpr const char SOURCE_META_DIR[] = "meta";

constexpr groupid_t INVALID_GROUPID = -1;
} // namespace indexlib::index

//////////////////////////////////////////////////////////////////////
namespace indexlibv2::index {
using indexlib::index::INVALID_GROUPID;
using indexlib::index::SOURCE_DATA_DIR_PREFIX;
using indexlib::index::SOURCE_DATA_FILE_NAME;
using indexlib::index::SOURCE_META_DIR;
using indexlib::index::SOURCE_OFFSET_FILE_NAME;
} // namespace indexlibv2::index
