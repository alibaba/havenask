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
inline const std::string PACK_ATTRIBUTE_INDEX_TYPE_STR = "pack_attribute";
inline const std::string PACK_ATTRIBUTE_INDEX_PATH = "attribute";
} // namespace indexlib::index

//////////////////////////////////////////////////////////////////////
namespace indexlibv2::index {
using indexlib::index::PACK_ATTRIBUTE_INDEX_PATH;
using indexlib::index::PACK_ATTRIBUTE_INDEX_TYPE_STR;
} // namespace indexlibv2::index
