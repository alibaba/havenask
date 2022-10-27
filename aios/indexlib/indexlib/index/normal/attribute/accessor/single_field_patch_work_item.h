#ifndef __INDEXLIB_SINGLE_FIELD_PATCH_WORK_ITEM_H
#define __INDEXLIB_SINGLE_FIELD_PATCH_WORK_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_work_item.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/util/mem_buffer.h"

DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);

IE_NAMESPACE_BEGIN(index);

class SingleFieldPatchWorkItem : public AttributePatchWorkItem
{
public:
    SingleFieldPatchWorkItem(
        const std::string& id, size_t patchItemCount, bool isSub, PatchWorkItemType itemType,
        const AttributeSegmentPatchIteratorPtr& patchSegIterator,
        docid_t baseDocId, uint32_t fieldId)
        : AttributePatchWorkItem(id, patchItemCount, isSub, itemType)
        , mPatchSegIter(patchSegIterator)
        , mBaseDocId(baseDocId)
        , mFieldId(fieldId)
    {}
    ~SingleFieldPatchWorkItem();
public:
    bool Init(const index::DeletionMapReaderPtr& deletionMapReader,
              const index::InplaceAttributeModifierPtr& attrModifier) override;    
    bool HasNext() const override;
    void ProcessNext() override;
    
    void destroy() override {}
    void drop() override {}

private:
    AttributeSegmentPatchIteratorPtr mPatchSegIter;
    docid_t mBaseDocId;
    uint32_t mFieldId;
    index::DeletionMapReaderPtr mDeletionMapReader;
    AttributeSegmentReaderPtr mAttrSegReader;
    util::MemBuffer mBuffer;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleFieldPatchWorkItem);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_FIELD_PATCH_WORK_ITEM_H
