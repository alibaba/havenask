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
#include "indexlib/index/kv/SortedMultiSegmentKVIterator.h"

#include <optional>
#include <queue>

#include "autil/mem_pool/Pool.h"
#include "indexlib/index/kv/KVKeyIterator.h"

namespace indexlibv2::index {

using RecordWithIndex = SortedMultiSegmentKVIterator::RecordWithIndex;
bool SortedMultiSegmentKVIterator::RecordWithIndexComparator::LessThan(const RecordWithIndex& lhs,
                                                                       const RecordWithIndex& rhs) const
{
    const auto& leftRecord = lhs.first;
    const auto& rightRecord = rhs.first;
    assert(lhs.second != rhs.second);
    if (leftRecord.deleted && rightRecord.deleted) {
        return lhs.second < rhs.second;
    } else if (leftRecord.deleted) {
        return true;
    } else if (rightRecord.deleted) {
        return false;
    } else {
        if (comp->ValueEqual(leftRecord, rightRecord)) {
            return lhs.second < rhs.second;
        } else {
            return comp->ValueLessThan(leftRecord, rightRecord);
        }
    }
}

SortedMultiSegmentKVIterator::SortedMultiSegmentKVIterator(
    schemaid_t targetSchemaId, std::shared_ptr<config::KVIndexConfig> indexConfig,
    std::shared_ptr<AdapterIgnoreFieldCalculator> ignoreFieldCalculator,
    std::vector<std::shared_ptr<framework::Segment>> segments, const config::SortDescriptions& sortDescriptions)
    : MultiSegmentKVIterator(targetSchemaId, std::move(indexConfig), std::move(ignoreFieldCalculator),
                             std::move(segments))
    , _sortDescriptions(sortDescriptions)
{
}

SortedMultiSegmentKVIterator::~SortedMultiSegmentKVIterator() {}

Status SortedMultiSegmentKVIterator::Init()
{
    auto comparator = std::make_unique<RecordComparator>();
    if (!comparator->Init(_indexConfig, _sortDescriptions)) {
        return Status::InternalError("failed to create RecordComparator");
    }
    _valueComp = std::move(comparator);

    for (size_t i = 0; i < _segments.size(); ++i) {
        std::unique_ptr<IKVIterator> iterator;
        auto s = CreateIterator(i, iterator);
        if (!s.IsOK()) {
            return s;
        }
        _iters.emplace_back(std::move(iterator));
        static constexpr size_t CHUNK_SIZE = 64 * 1024;
        static constexpr size_t ALIGN_SIZE = 8;
        _segmentPools.emplace_back(std::make_unique<autil::mem_pool::UnsafePool>(CHUNK_SIZE, ALIGN_SIZE));
    }

    auto s = CollectUnusedKeys();
    if (!s.IsOK()) {
        return s;
    }

    return InitValueHeap();
}

bool SortedMultiSegmentKVIterator::HasNext() const { return _valueHeap && !_valueHeap->empty(); }

Status SortedMultiSegmentKVIterator::Next(autil::mem_pool::Pool* pool, Record& record)
{
    auto& item = _valueHeap->top();
    record = item.first;
    auto segmentIndex = item.second;
    if (!record.deleted) {
        const size_t len = item.first.value.size();
        char* data = (char*)pool->allocate(len);
        memcpy(data, item.first.value.data(), len);
        record.value = autil::StringView(data, len);
    }
    _valueHeap->pop();

    auto s = PushNextRecordToHeap(segmentIndex);
    if (s.IsOK() || s.IsEof()) {
        return Status::OK();
    }
    return s;
}

Status SortedMultiSegmentKVIterator::Seek(offset_t segmentIndex, offset_t offset)
{
    return Status::Unimplement("SortedMultiSegmentKVIterator::Seek");
}

std::pair<offset_t, offset_t> SortedMultiSegmentKVIterator::GetOffset() const { return {-1, -1}; }

void SortedMultiSegmentKVIterator::Reset()
{
    _valueHeap.reset();
    _unusedKeys.clear();
    _segmentPools.clear();
    _iters.clear();
    _valueComp.reset();
}

Status SortedMultiSegmentKVIterator::CollectUnusedKeys()
{
    struct UnusedKeyCmp {
        using UnusedKey = SortedMultiSegmentKVIterator::UnusedKey;
        inline bool operator()(const UnusedKey& lhs, const UnusedKey& rhs) const { return !LessThan(lhs, rhs); }
        inline bool LessThan(const UnusedKey& lhs, const UnusedKey& rhs) const
        {
            return lhs.first == rhs.first ? lhs.second < rhs.second : lhs.first < rhs.first;
        }
    };
    std::priority_queue<UnusedKey, std::vector<UnusedKey>, UnusedKeyCmp> heap(UnusedKeyCmp {});

    std::vector<KVKeyIterator*> keyIters;
    keyIters.reserve(_segments.size());

    auto PushNextKeyToHeap = [this, &keyIters, &heap](uint32_t idx) {
        auto keyIter = keyIters[idx];
        if (!keyIter->HasNext()) {
            return Status::OK();
        }
        auto* pool = _segmentPools[idx].get();
        pool->reset();
        Record r;
        auto s = keyIter->Next(pool, r);
        if (s.IsOK()) {
            heap.emplace(r.key, idx);
        }
        return s;
    };

    for (uint32_t idx = 0; idx < _iters.size(); ++idx) {
        auto& iter = _iters[idx];
        KVKeyIterator* keyIter = dynamic_cast<KVKeyIterator*>(const_cast<IKVIterator*>(iter->GetKeyIterator()));
        if (!keyIter) {
            return Status::InternalError("key iterator is null");
        }
        keyIter->SortByKey();
        keyIters.push_back(keyIter);
    }

    // init heap
    for (uint32_t idx = 0; idx < _iters.size(); ++idx) {
        auto s = PushNextKeyToHeap(idx);
        if (!s.IsOK()) {
            return s;
        }
    }

    std::optional<keytype_t> lastKey;
    while (!heap.empty()) {
        const auto& item = heap.top();
        keytype_t key = item.first;
        uint32_t segmentIndex = item.second;
        if (lastKey.has_value() && lastKey.value() == key) {
            _unusedKeys.emplace(std::move(item));
        }
        lastKey = key;
        heap.pop();
        auto s = PushNextKeyToHeap(segmentIndex);
        if (!s.IsOK()) {
            return s;
        }
    }

    return Status::OK();
}

Status SortedMultiSegmentKVIterator::InitValueHeap()
{
    RecordWithIndexComparator cmp {_valueComp.get()};
    _valueHeap = std::make_unique<ValueHeap>(cmp);

    for (auto& iter : _iters) {
        KVKeyIterator* keyIter = dynamic_cast<KVKeyIterator*>(const_cast<IKVIterator*>(iter->GetKeyIterator()));
        if (!keyIter) {
            return Status::InternalError("key iterator is null");
        }
        keyIter->SortByValue();
    }

    for (auto idx = 0; idx < _iters.size(); ++idx) {
        auto& iter = _iters[idx];
        if (!iter->HasNext()) {
            continue;
        }
        auto s = PushNextRecordToHeap(idx);
        if (!s.IsOK() && !s.IsEof()) {
            return s;
        }
    }

    return Status::OK();
}

Status SortedMultiSegmentKVIterator::PushNextRecordToHeap(size_t segmentIndex)
{
    auto& iter = _iters[segmentIndex];
    auto* pool = _segmentPools[segmentIndex].get();

    while (iter->HasNext()) {
        // TODO: maybe reset when reach some threshold
        pool->reset();
        Record r;
        auto s = iter->Next(pool, r);
        if (!s.IsOK()) {
            return s;
        }
        if (!_unusedKeys.empty() && _unusedKeys.count({r.key, segmentIndex}) > 0) {
            continue;
        }
        _valueHeap->emplace(std::move(r), segmentIndex);
        return Status::OK();
    }
    return Status::Eof();
}

} // namespace indexlibv2::index
