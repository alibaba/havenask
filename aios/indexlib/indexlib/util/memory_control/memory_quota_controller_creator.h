#ifndef __INDEXLIB_MEMORY_QUOTA_CONTROLLER_CREATOR_H
#define __INDEXLIB_MEMORY_QUOTA_CONTROLLER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/memory_control/partition_memory_quota_controller.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"

IE_NAMESPACE_BEGIN(util);

class MemoryQuotaControllerCreator
{
public:
    MemoryQuotaControllerCreator() {}
    ~MemoryQuotaControllerCreator() {}
public:
    static MemoryQuotaControllerPtr CreateMemoryQuotaController(
            int64_t quota = std::numeric_limits<int64_t>::max())
    {
        return MemoryQuotaControllerPtr(new MemoryQuotaController(quota));
    }
    static PartitionMemoryQuotaControllerPtr CreatePartitionMemoryController(
            int64_t quota = std::numeric_limits<int64_t>::max())
    {
        return PartitionMemoryQuotaControllerPtr(new PartitionMemoryQuotaController(
                        CreateMemoryQuotaController(quota)));
    }
    static BlockMemoryQuotaControllerPtr CreateBlockMemoryController(
            int64_t quota = std::numeric_limits<int64_t>::max())
    {
        return BlockMemoryQuotaControllerPtr(new BlockMemoryQuotaController(
                        CreatePartitionMemoryController(quota)));
    }
    static SimpleMemoryQuotaControllerPtr CreateSimpleMemoryController(
            int64_t quota = std::numeric_limits<int64_t>::max())
    {
        return SimpleMemoryQuotaControllerPtr(new SimpleMemoryQuotaController(
                        CreateBlockMemoryController(quota)));
    }
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MemoryQuotaControllerCreator);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MEMORY_QUOTA_CONTROLLER_CREATOR_H
