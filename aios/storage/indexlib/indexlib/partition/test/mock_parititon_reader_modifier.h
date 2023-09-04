#ifndef __INDEXLIB_MOCK_PARITITON_READER_MODIFIER_H
#define __INDEXLIB_MOCK_PARITITON_READER_MODIFIER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/partition_reader_modifier.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace partition {

class MockPartitionReaderModifier : public PartitionReaderModifier
{
public:
    MockPartitionReaderModifier(const config::IndexPartitionSchemaPtr& schema, IndexPartitionReader* reader)
        : PartitionReaderModifier(schema, reader)
    {
    }

    MOCK_METHOD(bool, UpdateField, (docid_t docId, fieldid_t fieldId, const autil::StringView& value), (override));
    MOCK_METHOD(bool, RemoveDocument, (docid_t docId), (override));
};

DEFINE_SHARED_PTR(MockPartitionReaderModifier);
}} // namespace indexlib::partition

#endif //__INDEXLIB_MOCK_PARITITON_READER_MODIFIER_H
