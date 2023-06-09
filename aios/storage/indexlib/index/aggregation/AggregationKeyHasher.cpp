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
#include "indexlib/index/aggregation/AggregationKeyHasher.h"

#include <algorithm>

#include "autil/HashUtil.h"

namespace indexlibv2::index {

uint64_t AggregationKeyHasher::Hash(const std::string& key) { return Hash(autil::StringView(key)); }

uint64_t AggregationKeyHasher::Hash(const autil::StringView& key)
{
    return key.empty() ? 0 : autil::HashUtil::calculateHashValue(key);
}

uint64_t AggregationKeyHasher::Hash(const std::string& key1, const std::string& key2)
{
    return Hash(autil::StringView(key1), autil::StringView(key2));
}

uint64_t AggregationKeyHasher::Hash(const autil::StringView& key1, const autil::StringView& key2)
{
    if (key1.empty() && key2.empty()) {
        return 0;
    }
    size_t hash = Hash(key1);
    autil::HashUtil::combineHash(hash, key2);
    return hash;
}

uint64_t AggregationKeyHasher::Hash(const std::string& key1, const std::string& key2, const std::string& key3)
{
    return Hash(autil::StringView(key1), autil::StringView(key2), autil::StringView(key3));
}

uint64_t AggregationKeyHasher::Hash(const autil::StringView& key1, const autil::StringView& key2,
                                    const autil::StringView& key3)
{
    if (key1.empty() && key2.empty() && key3.empty()) {
        return 0;
    }
    uint64_t hash = Hash(key1, key2);
    autil::HashUtil::combineHash(hash, key3);
    return hash;
}

template <typename Iterator>
static uint64_t HashStrVec(Iterator begin, Iterator end)
{
    if (0 == std::distance(begin, end)) {
        return 0;
    }
    bool allEmpty = std::all_of(begin, end, [](const auto& key) { return key.empty(); });
    if (allEmpty) {
        return 0;
    }
    auto it = begin;
    uint64_t hash = AggregationKeyHasher::Hash(*it++);
    while (it < end) {
        autil::HashUtil::combineHash(hash, *it++);
    }
    return hash;
}

uint64_t AggregationKeyHasher::Hash(const std::vector<autil::StringView>& keys)
{
    return HashStrVec(keys.begin(), keys.end());
}

uint64_t AggregationKeyHasher::Hash(const std::vector<std::string>& keys)
{
    return HashStrVec(keys.begin(), keys.end());
}

} // namespace indexlibv2::index
