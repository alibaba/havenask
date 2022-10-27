#include "indexlib/partition/test/fake_index_partition_creator.h"
#include "indexlib/partition/test/mock_index_partition.h"

using namespace std;

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, FakeIndexPartitionCreator);

FakeIndexPartitionCreator::FakeIndexPartitionCreator() 
{
}

FakeIndexPartitionCreator::~FakeIndexPartitionCreator() 
{
}

IndexPartitionPtr FakeIndexPartitionCreator::Create(const config::IndexPartitionSchemaPtr &schema,
        const IndexPartitionReaderPtr &reader)
{
    MockIndexPartition *indexPart = new MockIndexPartition();
    indexPart->SetReader(reader);
    indexPart->SetSchema(schema);
    return IndexPartitionPtr(indexPart);
}


IE_NAMESPACE_END(partition);

