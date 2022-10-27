#ifndef __INDEXLIB_IN_MEMORY_SEGMENT_MODIFIER_H
#define __INDEXLIB_IN_MEMORY_SEGMENT_MODIFIER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index, InMemoryAttributeSegmentWriter);
DECLARE_REFERENCE_CLASS(index, DeletionMapSegmentWriter);
DECLARE_REFERENCE_CLASS(document, NormalDocument);

IE_NAMESPACE_BEGIN(partition);

class InMemorySegmentModifier
{
public:
    InMemorySegmentModifier();
    ~InMemorySegmentModifier();

public:
    void Init(index::DeletionMapSegmentWriterPtr deletionMapSegmentWriter,
              index::InMemoryAttributeSegmentWriterPtr attributeWriters);

    bool UpdateDocument(docid_t localDocId, 
                        const document::NormalDocumentPtr& doc);

    bool UpdateEncodedFieldValue(docid_t docId, fieldid_t fieldId,
                                 const autil::ConstString& value);

    void RemoveDocument(docid_t localDocId);

private:
    index::DeletionMapSegmentWriterPtr mDeletionMapSegmentWriter;
    index::InMemoryAttributeSegmentWriterPtr mAttributeWriters;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemorySegmentModifier);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_IN_MEMORY_SEGMENT_MODIFIER_H
