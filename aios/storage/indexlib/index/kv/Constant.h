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

#include "indexlib/index/kv/Types.h"

namespace indexlib::index {
static constexpr const char KV_KEY_FILE_NAME[] = "key";
static constexpr const char KV_VALUE_FILE_NAME[] = "value";
static constexpr const char KV_FORMAT_OPTION_FILE_NAME[] = "format_option";
static constexpr size_t DEFAULT_MAX_VALUE_SIZE_FOR_SHORT_OFFSET = (size_t)(std::numeric_limits<uint32_t>::max() - 1);
static constexpr regionid_t INVALID_REGIONID = (regionid_t)-1;
static constexpr regionid_t DEFAULT_REGIONID = (regionid_t)0;
static constexpr uint32_t MAX_EXPIRE_TIME = std::numeric_limits<uint32_t>::max();
} // namespace indexlib::index

//////////////////////////////////////////////////////////////////////
// TODO; rm
namespace indexlibv2::index {
using indexlib::index::KV_FORMAT_OPTION_FILE_NAME;
using indexlib::index::KV_KEY_FILE_NAME;
using indexlib::index::KV_VALUE_FILE_NAME;
} // namespace indexlibv2::index
