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

#include "indexlib/base/Status.h"

namespace indexlibv2::index {

class StatisticsTermFormatter
{
public:
    static std::string GetHeaderLine(const std::string& invertedIndexName, size_t termCount);
    static std::string GetDataLine(const std::string& term, size_t termHash);
    static Status ParseHeaderLine(const std::string& line, std::string& indexName, size_t& termCount);
    static Status ParseDataLine(const std::string& block, size_t& pos, std::string& term, size_t& termHash);
};

} // namespace indexlibv2::index
