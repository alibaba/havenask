#include "indexlib/index/normal/attribute/accessor/pack_field_patch_work_item.h"
#include "indexlib/index/normal/attribute/accessor/pack_field_patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/inplace_attribute_modifier.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PackFieldPatchWorkItem);

PackFieldPatchWorkItem::PackFieldPatchWorkItem(
    const std::string& id, size_t patchItemCount, bool isSub,
    PackFieldPatchIterator* packIterator)
    : AttributePatchWorkItem(id, patchItemCount, isSub, PWIT_PACK_ATTR)
    , mIter(packIterator)
{
}

PackFieldPatchWorkItem::~PackFieldPatchWorkItem() 
{
}

bool PackFieldPatchWorkItem::Init(
    const index::DeletionMapReaderPtr& deletionMapReader,
    const index::InplaceAttributeModifierPtr& attrModifier)
{
    if (!deletionMapReader)
    {
        IE_LOG(ERROR, "Init failed: DeletionMap reader is NULL");
        return false;
    }
    if (!attrModifier)
    {
        IE_LOG(ERROR, "Init failed: InplaceAttributeModifier is NULL");
        return false;
    }
    mDeletionMapReader = deletionMapReader;
    const auto& packAttrConfig = mIter->GetPackAttributeConfig();
    assert(!packAttrConfig->IsDisable());
    mPackAttrReader = attrModifier->GetPackAttributeReader(packAttrConfig->GetPackAttrId());
    if (!mPackAttrReader)
    {
        IE_LOG(ERROR, "get PackAttributeReader failed for PackAttrId[%u]",
               packAttrConfig->GetPackAttrId());
        return false;
    }
    mIter->Reserve(mBuffer);
    return true;
}

bool PackFieldPatchWorkItem::HasNext() const
{
    return mIter->HasNext();
}

void PackFieldPatchWorkItem::ProcessNext()
{
    assert(mIter);
    assert(mDeletionMapReader);
    assert(mPackAttrReader);
    assert(mIter->HasNext());
    mIter->Next(mBuffer);
    docid_t docId = mBuffer.GetDocId();
    if (docId == INVALID_DOCID || mDeletionMapReader->IsDeleted(docId))
    {
        return;
    }
    mPackAttrReader->UpdateField(docId, (uint8_t*)mBuffer.Data(), mBuffer.GetDataSize());    
}

IE_NAMESPACE_END(index);

