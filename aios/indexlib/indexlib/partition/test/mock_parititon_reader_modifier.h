#ifndef __INDEXLIB_MOCK_PARITITON_READER_MODIFIER_H
#define __INDEXLIB_MOCK_PARITITON_READER_MODIFIER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/partition_reader_modifier.h"

IE_NAMESPACE_BEGIN(partition);

class MockPartitionReaderModifier : public PartitionReaderModifier
{
public:
    MockPartitionReaderModifier(
            const config::IndexPartitionSchemaPtr& schema,
            IndexPartitionReader* reader)
        : PartitionReaderModifier(schema, reader)
    {}

    MOCK_METHOD3(UpdateField, bool(docid_t docId, fieldid_t fieldId, 
                                   const autil::ConstString& value));
    MOCK_METHOD1(RemoveDocument, bool(docid_t docId));
};

DEFINE_SHARED_PTR(MockPartitionReaderModifier);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MOCK_PARITITON_READER_MODIFIER_H
