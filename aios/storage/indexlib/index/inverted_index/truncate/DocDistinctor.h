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

#include "indexlib/index/inverted_index/truncate/TruncateAttributeReader.h"
#include "indexlib/util/HashMap.h"

namespace indexlib::index {

class DocDistinctor
{
public:
    DocDistinctor() = default;
    virtual ~DocDistinctor() = default;

public:
    virtual bool Distinct(docid_t docId) = 0;
    virtual bool IsFull() const = 0;
    virtual void Reset() = 0;
    virtual int64_t EstimateMemoryUse(uint32_t maxReserveDocCount, uint32_t totalDocCount) const = 0;
    virtual uint64_t GetLeftCnt() = 0;
};

template <typename T>
class DocDistinctorTyped : public DocDistinctor
{
public:
    DocDistinctorTyped(const std::shared_ptr<TruncateAttributeReader>& attrReader, uint64_t distCount)
    {
        _distCount = distCount;
        _truncateAttributeReader = attrReader;
        _ctx = _truncateAttributeReader->CreateReadContextPtr(&_pool);
        _distmap = std::make_shared<DistinctMap>(_distCount, 1024 * 1024);
    }
    ~DocDistinctorTyped()
    {
        _ctx.reset();
        _pool.release();
    };

public:
    bool Distinct(docid_t docId) override
    {
        T attrValue {};
        bool isNull = false;
        uint32_t dataLen = 0;
        _truncateAttributeReader->Read(docId, _ctx, (uint8_t*)&attrValue, sizeof(T), dataLen, isNull);
        if (!isNull) {
            _distmap->FindAndInsert(attrValue, true);
        }
        uint32_t curDistCount = _distmap->Size();
        if (curDistCount >= _distCount) {
            return true;
        }
        return false;
    }

    bool IsFull() const override { return _distmap->Size() >= _distCount; }

    void Reset() override { _distmap->Clear(); }

    int64_t EstimateMemoryUse(uint32_t maxReserveDocCount, uint32_t totalDocCount) const override
    {
        int64_t size = DistinctMap::EstimateNeededMemory(maxReserveDocCount);
        size += sizeof(totalDocCount) * sizeof(T); // attribute reader
        return size;
    }

    uint64_t GetLeftCnt() override
    {
        if (IsFull()) {
            return 0;
        } else {
            return _distCount - _distmap->Size();
        }
    }

private:
    using DistinctMap = util::HashMap<T, bool>;
    std::shared_ptr<DistinctMap> _distmap;

    uint64_t _distCount;
    std::shared_ptr<TruncateAttributeReader> _truncateAttributeReader;
    std::shared_ptr<indexlibv2::index::AttributeDiskIndexer::ReadContextBase> _ctx;
    autil::mem_pool::UnsafePool _pool;
};

} // namespace indexlib::index
