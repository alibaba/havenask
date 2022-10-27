#ifndef __INDEXLIB_DOC_DISTINCTOR_H
#define __INDEXLIB_DOC_DISTINCTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_attribute_reader.h"
#include "indexlib/util/hash_map.h"

IE_NAMESPACE_BEGIN(index);

class DocDistinctor
{
public:
    virtual ~DocDistinctor() {}

public:
    virtual bool Distinct(docid_t docId) = 0;
    virtual bool IsFull() = 0;
    virtual void Reset() = 0;
    virtual int64_t EstimateMemoryUse(uint32_t maxReserveDocCount,
            uint32_t totalDocCount) const = 0;

private:
    IE_LOG_DECLARE();
};

template <typename T>
class DocDistinctorTyped : public DocDistinctor
{
public:
    DocDistinctorTyped(TruncateAttributeReaderPtr attrReader,
                       uint64_t distCount)
    {
        mDistCount = distCount;
        mAttrReader = attrReader;
        mDistMap.reset(new DistinctMap(mDistCount, 1024 * 1024));
    }
    ~DocDistinctorTyped() {};
public:
    bool Distinct(docid_t docId)
    {
        T attrValue;
        mAttrReader->Read(docId, (uint8_t *)&attrValue, sizeof(T));
        mDistMap->FindAndInsert(attrValue, true);
        uint32_t curDistCount = mDistMap->Size();
        if (curDistCount >= mDistCount)
        {
            return true;
        }
        return false;
    }

    bool IsFull()
    {
        return mDistMap->Size() >= mDistCount;
    }

    virtual void Reset()
    {
        mDistMap->Clear();
    }

    virtual int64_t EstimateMemoryUse(uint32_t maxReserveDocCount, uint32_t totalDocCount) const
    {
        int64_t size = DistinctMap::EstimateNeededMemory(maxReserveDocCount);
        size += sizeof(totalDocCount) * sizeof(T); // attribute reader
        return size;
    }

private:
    typedef util::HashMap<T, bool> DistinctMap;
    typedef std::tr1::shared_ptr<DistinctMap> DistinctMapPtr;

    DistinctMapPtr mDistMap;
    uint64_t mDistCount;
    TruncateAttributeReaderPtr mAttrReader;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocDistinctor);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_DISTINCTOR_H
