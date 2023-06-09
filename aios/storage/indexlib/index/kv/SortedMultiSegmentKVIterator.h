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

#include <memory>
#include <queue>
#include <vector>

#include "indexlib/index/kv/MultiSegmentKVIterator.h"
#include "indexlib/index/kv/RecordComparator.h"

namespace indexlibv2::index {

class SortedMultiSegmentKVIterator final : public MultiSegmentKVIterator
{
public:
    SortedMultiSegmentKVIterator(schemaid_t targetSchemaId, std::shared_ptr<config::KVIndexConfig> indexConfig,
                                 std::shared_ptr<AdapterIgnoreFieldCalculator> ignoreFieldCalculator,
                                 std::vector<std::shared_ptr<framework::Segment>> segments,
                                 const config::SortDescriptions& sortDescriptions);
    ~SortedMultiSegmentKVIterator();

public:
    Status Init() override;

    bool HasNext() const override;
    Status Next(autil::mem_pool::Pool* pool, Record& record) override;
    Status Seek(offset_t segmentIndex, offset_t offset) override;
    std::pair<offset_t, offset_t> GetOffset() const override;
    void Reset() override;

private:
    Status CollectUnusedKeys();
    Status InitValueHeap();
    Status PushNextRecordToHeap(size_t segmentIndex);

public:
    using UnusedKey = std::pair<keytype_t, uint32_t>;
    using RecordWithIndex = std::pair<Record, uint32_t>;
    struct RecordWithIndexComparator {
        RecordComparator* comp = nullptr;
        bool operator()(const RecordWithIndex& lhs, const RecordWithIndex& rhs) const { return !LessThan(lhs, rhs); }
        bool LessThan(const RecordWithIndex& lhs, const RecordWithIndex& rhs) const;
    };
    using ValueHeap = std::priority_queue<RecordWithIndex, std::vector<RecordWithIndex>, RecordWithIndexComparator>;

private:
    config::SortDescriptions _sortDescriptions;
    std::unique_ptr<RecordComparator> _valueComp;
    std::vector<std::unique_ptr<IKVIterator>> _iters;
    std::vector<std::unique_ptr<autil::mem_pool::Pool>> _segmentPools;
    std::set<UnusedKey> _unusedKeys; // TODO: use a memory efficient hash table
    std::unique_ptr<ValueHeap> _valueHeap;
};

} // namespace indexlibv2::index
