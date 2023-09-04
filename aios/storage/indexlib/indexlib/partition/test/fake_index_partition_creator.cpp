#include "indexlib/partition/test/fake_index_partition_creator.h"

#include "indexlib/partition/test/mock_index_partition.h"

using namespace std;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, FakeIndexPartitionCreator);

FakeIndexPartitionCreator::FakeIndexPartitionCreator() {}

FakeIndexPartitionCreator::~FakeIndexPartitionCreator() {}

IndexPartitionPtr FakeIndexPartitionCreator::Create(const config::IndexPartitionSchemaPtr& schema,
                                                    const IndexPartitionReaderPtr& reader)
{
    IndexPartitionResource partitionResource = MockIndexPartition::CreatePartitionResource();
    MockIndexPartition* indexPart = new MockIndexPartition(partitionResource);
    indexPart->SetReader(reader);
    indexPart->SetSchema(schema);
    return IndexPartitionPtr(indexPart);
}
}} // namespace indexlib::partition
