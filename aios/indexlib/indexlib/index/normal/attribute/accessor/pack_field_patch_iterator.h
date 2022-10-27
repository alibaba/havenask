#ifndef __INDEXLIB_PACK_FIELD_PATCH_ITERATOR_H
#define __INDEXLIB_PACK_FIELD_PATCH_ITERATOR_H

#include <tr1/memory>
#include <queue>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index/normal/attribute/accessor/single_field_patch_iterator.h"

IE_NAMESPACE_BEGIN(index);

class SingleFieldPatchIteratorComparator
{
public:
    bool operator () (SingleFieldPatchIterator*& lhs,
                      SingleFieldPatchIterator*& rhs) const
    {
        assert(lhs);
        assert(rhs);

        if (lhs->GetCurrentDocId() != rhs->GetCurrentDocId())
        {
            return lhs->GetCurrentDocId() > rhs->GetCurrentDocId();
        }
        return lhs->GetFieldId() > rhs->GetFieldId();
    }
};

class PackFieldPatchIterator : public AttributePatchIterator
{
public:
    PackFieldPatchIterator(
        const config::PackAttributeConfigPtr& packAttrConfig, bool isSub)
        : AttributePatchIterator(APT_PACK, isSub)
        , mPackAttrConfig(packAttrConfig)
        , mPatchLoadExpandSize(0)
        , mPatchItemCount(0) {}
    
    ~PackFieldPatchIterator();
    
public:
    void Init(const index_base::PartitionDataPtr& partitionData,
              bool isIncConsistentWithRt,
              const index_base::Version& lastLoadedVersion,
              segmentid_t startLoadSegment);
    
    void Next(AttrFieldValue& value) override;
    void Reserve(AttrFieldValue& value) override;
    size_t GetPatchLoadExpandSize() const override
    { return mPatchLoadExpandSize; }

    size_t GetPatchItemCount() const
    { return mPatchItemCount; }    
    void CreateIndependentPatchWorkItems(std::vector<AttributePatchWorkItem*>& workItems);
    const config::PackAttributeConfigPtr& GetPackAttributeConfig() const
    {
        return mPackAttrConfig;
    }
private:
    SingleFieldPatchIterator* CreateSingleFieldPatchIterator(
        const index_base::PartitionDataPtr& partitionData,
        bool isIncConsistentWithRt,
        segmentid_t startLoadSegment,
        const config::AttributeConfigPtr& attrConf);

    docid_t GetNextDocId() const
    {
        if (mHeap.empty())
        {
            return INVALID_DOCID;
        }
        SingleFieldPatchIterator* iter = mHeap.top();
        assert(iter);
        return iter->GetCurrentDocId();
    }
    
private:
    typedef std::priority_queue<SingleFieldPatchIterator*,
        std::vector<SingleFieldPatchIterator*>,
        SingleFieldPatchIteratorComparator> SingleFieldPatchIteratorHeap;
    
private:
    config::PackAttributeConfigPtr mPackAttrConfig;
    SingleFieldPatchIteratorHeap mHeap;
    std::vector<AttrFieldValuePtr> mPatchValues;
    size_t mPatchLoadExpandSize;
    size_t mPatchItemCount;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackFieldPatchIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACK_FIELD_PATCH_ITERATOR_H
