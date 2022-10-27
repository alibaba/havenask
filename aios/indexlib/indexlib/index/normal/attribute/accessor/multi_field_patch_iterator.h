#ifndef __INDEXLIB_MULTI_FIELD_PATCH_ITERATOR_H
#define __INDEXLIB_MULTI_FIELD_PATCH_ITERATOR_H

#include <tr1/memory>
#include <queue>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_iterator.h"

IE_NAMESPACE_BEGIN(index);

class AttributePatchIteratorComparator
{
public:
    bool operator() (AttributePatchIterator*& lft,
                     AttributePatchIterator*& rht) const
    {
        assert(lft);
        assert(rht);
        return (*rht) < (*lft);
    }
};

class MultiFieldPatchIterator : public PatchIterator
{
public:
    MultiFieldPatchIterator(const config::IndexPartitionSchemaPtr& schema);
    ~MultiFieldPatchIterator();

public:
    void Init(const index_base::PartitionDataPtr& partitionData,
              bool isIncConsistentWithRt,
              const index_base::Version& lastLoadVersion,
              segmentid_t startLoadSegment = INVALID_SEGMENTID,
              bool isSub = false);

    void Next(AttrFieldValue& value) override;
    void Reserve(AttrFieldValue& value) override;
    bool HasNext() const override { return !mHeap.empty(); }
    size_t GetPatchLoadExpandSize() const override
    { return mPatchLoadExpandSize; };

    docid_t GetCurrentDocId() const 
    {
        if (!HasNext())
        {
            return INVALID_DOCID;
        }
        AttributePatchIterator* patchIter = mHeap.top();
        assert(patchIter);
        return patchIter->GetCurrentDocId();
    }

    void CreateIndependentPatchWorkItems(std::vector<AttributePatchWorkItem*>& workItems);

private:
    AttributePatchIterator* CreateSingleFieldPatchIterator(
        const index_base::PartitionDataPtr& partitionData,
        bool isIncConsistentWithRt,
        segmentid_t startLoadSegment,
        const config::AttributeConfigPtr& attrConf);

    AttributePatchIterator* CreatePackFieldPatchIterator(
        const index_base::PartitionDataPtr& partitionData,
        bool isIncConsistentWithRt,
        const index_base::Version& lastLoadVersion,
        segmentid_t startLoadSegment,
        const config::PackAttributeConfigPtr& packAttrConf);

private:
    typedef std::priority_queue<AttributePatchIterator*, 
        std::vector<AttributePatchIterator*>,
        AttributePatchIteratorComparator> AttributePatchReaderHeap;

private:
    config::IndexPartitionSchemaPtr mSchema;
    AttributePatchReaderHeap mHeap;
    docid_t mCurDocId;
    bool mIsSub;
    size_t mPatchLoadExpandSize;

private:
    friend class MultiFieldPatchIteratorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiFieldPatchIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_FIELD_PATCH_ITERATOR_H
