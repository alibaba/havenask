#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index_define.h"
#include "indexlib/util/mem_buffer.h"

IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);

void AttributeSegmentReader::LoadPatches(
        const AttributeSegmentPatchIteratorPtr& patchIter)
{
    assert(patchIter);

    MemBuffer buffer;
    buffer.Reserve(patchIter->GetMaxPatchItemLen());
    while(patchIter->HasNext())
    {
        docid_t docId = INVALID_DOCID;
        size_t valueSize = patchIter->Next(docId, 
                (uint8_t*)buffer.GetBuffer(), buffer.GetBufferSize());
        UpdateField(docId, (uint8_t*)buffer.GetBuffer(), valueSize);
    }
}

IE_NAMESPACE_END(index);

