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
inline const std::string KV_INDEX_TYPE_STR = "kv";
inline const std::string KV_INDEX_PATH = "index";
inline const std::string DROP_DELETE_KEY = "drop_delete_key";
inline const std::string KV_RAW_KEY_INDEX_NAME = "raw_key";
inline const std::string CURRENT_TIME_IN_SECOND = "current_time_in_sec";
inline const std::string NEED_STORE_PK_VALUE = "need_store_pk_value";
} // namespace indexlib::index

//////////////////////////////////////////////////////////////////////
namespace indexlibv2::index {
using indexlib::index::CURRENT_TIME_IN_SECOND;
using indexlib::index::DROP_DELETE_KEY;
using indexlib::index::KV_INDEX_PATH;
using indexlib::index::KV_INDEX_TYPE_STR;
using indexlib::index::KV_RAW_KEY_INDEX_NAME;
using indexlib::index::NEED_STORE_PK_VALUE;
} // namespace indexlibv2::index
