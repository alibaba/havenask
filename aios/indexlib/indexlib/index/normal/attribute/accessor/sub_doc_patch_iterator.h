#ifndef __INDEXLIB_SUB_DOC_PATCH_ITERATOR_H
#define __INDEXLIB_SUB_DOC_PATCH_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/config/index_partition_schema.h"

IE_NAMESPACE_BEGIN(index);

class SubDocPatchIterator : public PatchIterator
{
public:
    SubDocPatchIterator(const config::IndexPartitionSchemaPtr& schema);
    ~SubDocPatchIterator();
public:
    void Init(const index_base::PartitionDataPtr& partitionData,
              bool isIncConsistentWithRt,
              const index_base::Version& lastLoadVersion,
              segmentid_t startLoadSegment = INVALID_SEGMENTID);


    void Next(AttrFieldValue& value) override;

    bool HasNext() const override
    { return mMainIterator.HasNext() || mSubIterator.HasNext(); }

    void Reserve(AttrFieldValue& value) override
    {
        mMainIterator.Reserve(value);
        mSubIterator.Reserve(value);
    }

    size_t GetPatchLoadExpandSize() const override
    {
        return mMainIterator.GetPatchLoadExpandSize() +
            mSubIterator.GetPatchLoadExpandSize();
    }
    void CreateIndependentPatchWorkItems(std::vector<AttributePatchWorkItem*>& workItems);    

private:
    bool LessThen(docid_t mainDocId, docid_t subDocId) const;

private:
    config::IndexPartitionSchemaPtr mSchema;
    MultiFieldPatchIterator mMainIterator;
    MultiFieldPatchIterator mSubIterator;
    JoinDocidAttributeReaderPtr mSubJoinAttributeReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocPatchIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SUB_DOC_PATCH_ITERATOR_H
