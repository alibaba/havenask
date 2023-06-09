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

#include <vector>

#include "autil/StringView.h"

namespace indexlibv2::index {

class AggregationKeyHasher
{
public:
    static uint64_t Hash(const std::string& key);
    static uint64_t Hash(const autil::StringView& key);

    static uint64_t Hash(const std::string& key1, const std::string& key2);
    static uint64_t Hash(const autil::StringView& key1, const autil::StringView& key2);

    static uint64_t Hash(const std::string& key1, const std::string& key2, const std::string& key3);
    static uint64_t Hash(const autil::StringView& key1, const autil::StringView& key2, const autil::StringView& key3);

    static uint64_t Hash(const std::vector<autil::StringView>& keys);
    static uint64_t Hash(const std::vector<std::string>& keys);
};

} // namespace indexlibv2::index
