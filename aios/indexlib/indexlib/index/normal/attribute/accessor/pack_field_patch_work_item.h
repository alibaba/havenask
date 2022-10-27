#ifndef __INDEXLIB_PACK_FIELD_PATCH_WORK_ITEM_H
#define __INDEXLIB_PACK_FIELD_PATCH_WORK_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_work_item.h"

DECLARE_REFERENCE_CLASS(index, PackFieldPatchIterator);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);
IE_NAMESPACE_BEGIN(index);

class PackFieldPatchWorkItem : public AttributePatchWorkItem
{
public:
    PackFieldPatchWorkItem(
        const std::string& id, size_t patchItemCount, bool isSub,
        PackFieldPatchIterator* packIterator);
    ~PackFieldPatchWorkItem();
public:
    bool Init(const index::DeletionMapReaderPtr& deletionMapReader,
              const index::InplaceAttributeModifierPtr& attrModifier) override;

    void destroy() override {}
    void drop() override {}
    bool HasNext() const override;
    void ProcessNext() override;
    
private:
    PackFieldPatchIterator* mIter;
    index::DeletionMapReaderPtr mDeletionMapReader;    
    PackAttributeReaderPtr mPackAttrReader;
    AttrFieldValue mBuffer;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackFieldPatchWorkItem);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACK_FIELD_PATCH_WORK_ITEM_H
