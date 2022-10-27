#ifndef __INDEXLIB_SHARDING_PARTITIONER_CREATOR_H
#define __INDEXLIB_SHARDING_PARTITIONER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index, ShardingPartitioner);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(index);

class ShardingPartitionerCreator
{
public:
    ShardingPartitionerCreator();
    ~ShardingPartitionerCreator();
    
public:
    static ShardingPartitioner* Create(
            const config::IndexPartitionSchemaPtr& schema,
            uint32_t shardingColumnCount);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ShardingPartitionerCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SHARDING_PARTITIONER_CREATOR_H
