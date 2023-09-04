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
namespace indexlib::index {
inline const std::string INVERTED_INDEX_TYPE_STR = "inverted_index";
inline const std::string INVERTED_INDEX_PATH = "index";
inline const std::string OPTIMIZE_MERGE = "optimize_merge";
inline const std::string RANGE_INFO_FILE_NAME = "range_info";
inline const std::string GENERAL_INVERTED_INDEX_TYPE_STR = "general_inverted_index"; // pk + inverted + ann
} // namespace indexlib::index

//////////////////////////////////////////////////////////////////////
namespace indexlibv2::index {
using indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR;
using indexlib::index::INVERTED_INDEX_TYPE_STR;
} // namespace indexlibv2::index
