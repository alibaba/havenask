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
#include "indexlib/index/kv/RecordComparator.h"

#include "autil/Log.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, RecordComparator);

RecordComparator::RecordComparator() = default;

RecordComparator::~RecordComparator() = default;

bool RecordComparator::Init(const std::shared_ptr<config::KVIndexConfig>& kvIndexConfig,
                            const config::SortDescriptions& sortDescriptions)
{
    auto valueConfig = kvIndexConfig->GetValueConfig();
    return _valueComparator.Init(valueConfig, sortDescriptions);
}

bool RecordComparator::operator()(const Record& lfr, const Record& rhr) const
{
    if (lfr.deleted && rhr.deleted) {
        return lfr.key < rhr.key;
    }
    if (lfr.deleted) {
        return true;
    }
    if (rhr.deleted) {
        return false;
    }
    int r = _valueComparator.Compare(lfr.value, rhr.value);
    return r == 0 ? (lfr.key < rhr.key) : (r < 0);
}

bool RecordComparator::ValueEqual(const Record& lhs, const Record& rhs) const
{
    return 0 == _valueComparator.Compare(lhs.value, rhs.value);
}

bool RecordComparator::ValueLessThan(const Record& lhs, const Record& rhs) const
{
    return _valueComparator.Compare(lhs.value, rhs.value) < 0;
}

} // namespace indexlibv2::index
