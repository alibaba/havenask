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
#ifndef __INDEXLIB_DOC_DISTINCTOR_H
#define __INDEXLIB_DOC_DISTINCTOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/HashMap.h"

namespace indexlib::index::legacy {

class DocDistinctor
{
public:
    virtual ~DocDistinctor() {}

public:
    virtual bool Distinct(docid_t docId) = 0;
    virtual bool IsFull() = 0;
    virtual void Reset() = 0;
    virtual int64_t EstimateMemoryUse(uint32_t maxReserveDocCount, uint32_t totalDocCount) const = 0;
    virtual uint64_t GetLeftCnt() = 0;

private:
    IE_LOG_DECLARE();
};

template <typename T>
class DocDistinctorTyped : public DocDistinctor
{
public:
    DocDistinctorTyped(TruncateAttributeReaderPtr attrReader, uint64_t distCount)
    {
        mDistCount = distCount;
        mAttrReader = attrReader;
        mCtx = mAttrReader->CreateReadContextPtr(&mPool);
        mDistMap.reset(new DistinctMap(mDistCount, 1024 * 1024));
    }
    ~DocDistinctorTyped()
    {
        mCtx.reset();
        mPool.release();
    };

public:
    bool Distinct(docid_t docId)
    {
        T attrValue {};
        bool isNull = false;
        mAttrReader->Read(docId, mCtx, (uint8_t*)&attrValue, sizeof(T), isNull);
        if (!isNull) {
            mDistMap->FindAndInsert(attrValue, true);
        }
        uint32_t curDistCount = mDistMap->Size();
        if (curDistCount >= mDistCount) {
            return true;
        }
        return false;
    }

    bool IsFull() { return mDistMap->Size() >= mDistCount; }

    uint64_t GetLeftCnt()
    {
        if (IsFull()) {
            return 0;
        } else {
            return mDistCount - mDistMap->Size();
        }
    }

    virtual void Reset() { mDistMap->Clear(); }

    virtual int64_t EstimateMemoryUse(uint32_t maxReserveDocCount, uint32_t totalDocCount) const
    {
        int64_t size = DistinctMap::EstimateNeededMemory(maxReserveDocCount);
        size += sizeof(totalDocCount) * sizeof(T); // attribute reader
        return size;
    }

private:
    typedef util::HashMap<T, bool> DistinctMap;
    typedef std::shared_ptr<DistinctMap> DistinctMapPtr;

    DistinctMapPtr mDistMap;
    uint64_t mDistCount;
    TruncateAttributeReaderPtr mAttrReader;
    AttributeSegmentReader::ReadContextBasePtr mCtx;
    autil::mem_pool::UnsafePool mPool;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocDistinctor);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_DOC_DISTINCTOR_H
