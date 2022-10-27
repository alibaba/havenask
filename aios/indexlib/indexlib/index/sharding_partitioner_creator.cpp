#include "indexlib/index/sharding_partitioner_creator.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, ShardingPartitionerCreator);

ShardingPartitionerCreator::ShardingPartitionerCreator()
{
}

ShardingPartitionerCreator::~ShardingPartitionerCreator()
{
}

ShardingPartitioner* ShardingPartitionerCreator::Create(
        const IndexPartitionSchemaPtr& schema,
        uint32_t shardingColumnCount)
{
    return NULL;
}

IE_NAMESPACE_END(index);
