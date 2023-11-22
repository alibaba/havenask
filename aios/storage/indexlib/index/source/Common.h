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
#include "indexlib/index/source/Constant.h"
#include "indexlib/index/source/Types.h"

namespace indexlib::index {
inline const std::string SOURCE_INDEX_TYPE_STR = "source";
inline const std::string SOURCE_INDEX_NAME = "source";
inline const std::string SOURCE_INDEX_PATH = "source";

} // namespace indexlib::index

//////////////////////////////////////////////////////////////////////
namespace indexlibv2::index {
using indexlib::index::SOURCE_INDEX_NAME;
using indexlib::index::SOURCE_INDEX_PATH;
using indexlib::index::SOURCE_INDEX_TYPE_STR;
static inline std::string GetDataDir(sourcegroupid_t groupId)
{
    return std::string(SOURCE_DATA_DIR_PREFIX) + "_" + std::to_string(groupId);
}
} // namespace indexlibv2::index
