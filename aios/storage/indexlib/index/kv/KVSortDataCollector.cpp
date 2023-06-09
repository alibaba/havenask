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
#include "indexlib/index/kv/KVSortDataCollector.h"

using namespace std;

namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, KVSortDataCollector);

// TODO(xinfei.sxf) use external pool
KVSortDataCollector::KVSortDataCollector() : _records(&_pool) {}

KVSortDataCollector::~KVSortDataCollector() { _records.clear(); }

bool KVSortDataCollector::Init(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                               const config::SortDescriptions& sortDescriptions)
{
    auto comparator = std::make_unique<RecordComparator>();
    if (!comparator->Init(kvIndexConfig, sortDescriptions)) {
        return false;
    }
    _comparator = std::move(comparator);
    return true;
}

void KVSortDataCollector::Sort()
{
    auto cmp = [this](Record& lhs, Record& rhs) { return (*_comparator)(lhs, rhs); };
    std::sort(_records.begin(), _records.end(), std::move(cmp));
}

void KVSortDataCollector::UpdateMemoryUsage(MemoryUsage& memoryUsage) const
{
    memoryUsage.buildMemory = _pool.getTotalBytes();
}

} // namespace indexlibv2::index
