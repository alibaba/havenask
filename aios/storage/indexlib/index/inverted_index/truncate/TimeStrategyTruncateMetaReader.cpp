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
#include "indexlib/index/inverted_index/truncate/TimeStrategyTruncateMetaReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, TimeStrategyTruncateMetaReader);

TimeStrategyTruncateMetaReader::TimeStrategyTruncateMetaReader(int64_t minTime, int64_t maxTime, bool desc)
    : TruncateMetaReader(desc)
    , _minTime(minTime)
    , _maxTime(maxTime)
{
}

bool TimeStrategyTruncateMetaReader::Lookup(const index::DictKeyInfo& key, int64_t& min, int64_t& max) const
{
    auto it = _dict.find(key);
    if (it == _dict.end()) {
        return false;
    }

    if (_desc) {
        min = std::min(_minTime, it->second.first);
        max = _maxTime;
    } else {
        min = _minTime;
        max = std::max(_maxTime, it->second.second);
    }
    return true;
}

} // namespace indexlib::index
