#ifndef __INDEXLIB_SINGLE_FIELD_PATCH_ITERATOR_H
#define __INDEXLIB_SINGLE_FIELD_PATCH_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index_base/patch/patch_file_finder.h"

IE_NAMESPACE_BEGIN(index);

class SingleFieldPatchIterator : public AttributePatchIterator
{
public:
    typedef std::pair<docid_t, AttributeSegmentPatchIteratorPtr> SegmentPatchIteratorInfo;

public:
    SingleFieldPatchIterator(
        const config::AttributeConfigPtr& attrConfig, bool isSub);

    ~SingleFieldPatchIterator();

public:
    void Init(const index_base::PartitionDataPtr& partitionData,
              bool isIncConsistentWithRt, segmentid_t startLoadSegment);
    void Next(AttrFieldValue& value) override;
    void Reserve(AttrFieldValue& value) override;
    size_t GetPatchLoadExpandSize() const override;

    fieldid_t GetFieldId() const { return mAttrIdentifier; }

    attrid_t GetAttributeId() const;

    void CreateIndependentPatchWorkItems(std::vector<AttributePatchWorkItem*>& workItems);

    size_t GetPatchItemCount() const
    {
        return mPatchItemCount;
    }
    
private:
    AttributeSegmentPatchIteratorPtr CreateSegmentPatchIterator(
            const index_base::AttrPatchFileInfoVec& patchInfoVec);

    docid_t CalculateGlobalDocId() const;

    index_base::AttrPatchFileInfoVec FilterLoadedPatchFileInfos(
            index_base::AttrPatchFileInfoVec& patchInfosVec, 
            segmentid_t lastLoadedSegment);

    docid_t GetNextDocId() const
    {
        return mCursor < mSegmentPatchIteratorInfos.size() ?
            CalculateGlobalDocId() : INVALID_DOCID;
    }
    
private:
    std::vector<SegmentPatchIteratorInfo> mSegmentPatchIteratorInfos;

    config::AttributeConfigPtr mAttrConfig;
    size_t mCursor;
    size_t mPatchLoadExpandSize;
    size_t mPatchItemCount;

private:
    friend class SingleFieldPatchIteratorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleFieldPatchIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_FIELD_PATCH_ITERATOR_H
