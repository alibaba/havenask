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

#include "autil/Log.h"
#include "autil/mem_pool/Map.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/Vector.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/MemoryUsage.h"
#include "indexlib/index/kv/Record.h"
#include "indexlib/index/kv/RecordComparator.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

namespace indexlibv2::index {

class KVSortDataCollector
{
public:
    KVSortDataCollector();
    ~KVSortDataCollector();

public:
    bool Init(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
              const config::SortDescriptions& sortDescriptions);
    void Reserve(size_t size) { _records.reserve(size); }
    void Collect(Record& record) { _records.emplace_back(record); }
    void Sort();
    size_t Size() { return _records.size(); }
    auto& GetRecords() { return _records; };
    const auto& GetRecords() const { return _records; }
    void UpdateMemoryUsage(MemoryUsage& memoryUsage) const;

public:
    auto& TEST_GetRecords() { return _records; }
    const std::unique_ptr<RecordComparator>& TEST_GetRecordComparator() const { return _comparator; }

private:
    autil::mem_pool::UnsafePool _pool;
    autil::mem_pool::Vector<Record> _records;
    std::unique_ptr<RecordComparator> _comparator;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
