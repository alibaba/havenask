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
#include "indexlib/index/merger_util/truncate/time_strategy_truncate_meta_reader.h"

using namespace std;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, TimeStrategyTruncateMetaReader);

TimeStrategyTruncateMetaReader::TimeStrategyTruncateMetaReader(int64_t minTime, int64_t maxTime, bool desc)
    : TruncateMetaReader(desc)
    , mMinTime(minTime)
    , mMaxTime(maxTime)
{
}

TimeStrategyTruncateMetaReader::~TimeStrategyTruncateMetaReader() {}

bool TimeStrategyTruncateMetaReader::Lookup(const index::DictKeyInfo& key, int64_t& min, int64_t& max)
{
    DictType::iterator it = mDict.find(key);
    if (it == mDict.end()) {
        return false;
    }

    if (mDesc) {
        min = std::min(mMinTime, it->second.first);
        max = mMaxTime;
    } else {
        min = mMinTime;
        max = std::max(mMaxTime, it->second.second);
    }
    return true;
}
} // namespace indexlib::index::legacy
