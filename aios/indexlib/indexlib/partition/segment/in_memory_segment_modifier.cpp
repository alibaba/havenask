#include "indexlib/partition/segment/in_memory_segment_modifier.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/accessor/in_memory_attribute_segment_writer.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, InMemorySegmentModifier);

InMemorySegmentModifier::InMemorySegmentModifier() 
{
}

InMemorySegmentModifier::~InMemorySegmentModifier() 
{
}

void InMemorySegmentModifier::Init(
        DeletionMapSegmentWriterPtr deletionMapSegmentWriter,
        InMemoryAttributeSegmentWriterPtr attributeWriters)
{
    mDeletionMapSegmentWriter = deletionMapSegmentWriter;
    mAttributeWriters = attributeWriters;
}

bool InMemorySegmentModifier::UpdateDocument(
        docid_t localDocId, const NormalDocumentPtr& doc)
{
    if (mAttributeWriters)
    {
        return mAttributeWriters->UpdateDocument(localDocId, doc);
    }
    IE_LOG(WARN, "no attribute writers");
    return false;
}

bool InMemorySegmentModifier::UpdateEncodedFieldValue(
        docid_t docId, fieldid_t fieldId, const ConstString& value)
{
    if (mAttributeWriters)
    {
        return mAttributeWriters->UpdateEncodedFieldValue(docId, fieldId, value);
    }
    IE_LOG(WARN, "no attribute writers");
    return false;
}

void InMemorySegmentModifier::RemoveDocument(docid_t localDocId)
{
    assert(mDeletionMapSegmentWriter);
    mDeletionMapSegmentWriter->Delete(localDocId);
}

IE_NAMESPACE_END(partition);

