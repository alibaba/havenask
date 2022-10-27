#include "indexlib/index/sharding_partitioner.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, ShardingPartitioner);

ShardingPartitioner::ShardingPartitioner()
    : mKeyHasherType(hft_unknown)
    , mShardingMask(0)
    , mIsMultiRegion(false)
{
}

ShardingPartitioner::~ShardingPartitioner() 
{
}

void ShardingPartitioner::Init(
        const IndexPartitionSchemaPtr& schema,
        uint32_t shardingColumnCount)
{
    assert(schema);
    assert(shardingColumnCount > 1);

    if (schema->GetRegionCount() > 1)
    {
        mIsMultiRegion = true;
    }
    else
    {
        mIsMultiRegion = false;
        InitKeyHasherType(schema);
    }
    
    uint32_t priceNum = 1;
    while (priceNum < shardingColumnCount)
    {
        priceNum <<= 1;
    }
    
    if (priceNum != shardingColumnCount)
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "shardingColumnCount[%u] should be 2^n", shardingColumnCount);
    }
    mShardingMask = shardingColumnCount - 1;
}

IE_NAMESPACE_END(index);

