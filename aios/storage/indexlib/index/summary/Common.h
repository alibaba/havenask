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
inline const std::string SUMMARY_INDEX_TYPE_STR = "summary";
inline const std::string SUMMARY_INDEX_NAME = "summary";
inline const std::string SUMMARY_INDEX_PATH = "summary";
} // namespace indexlib::index

namespace indexlibv2::index {
using indexlib::index::SUMMARY_INDEX_NAME;
using indexlib::index::SUMMARY_INDEX_PATH;
using indexlib::index::SUMMARY_INDEX_TYPE_STR;
} // namespace indexlibv2::index
