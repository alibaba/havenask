#ifndef __INDEXLIB_MOCK_PARTITION_RESOURCE_CALCULATOR_H
#define __INDEXLIB_MOCK_PARTITION_RESOURCE_CALCULATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/partition_resource_calculator.h"

IE_NAMESPACE_BEGIN(partition);

class MockPartitionResourceCalculator : public PartitionResourceCalculator
{
public:
    MockPartitionResourceCalculator(
            const config::OnlineConfig& onlineConfig)
        : PartitionResourceCalculator(onlineConfig)
    {}

    ~MockPartitionResourceCalculator() {}

public:
    MOCK_CONST_METHOD0(GetCurrentMemoryUse, size_t());
    MOCK_CONST_METHOD0(GetRtIndexMemoryUse, size_t());
    MOCK_CONST_METHOD0(GetWriterMemoryUse, size_t());
    MOCK_CONST_METHOD0(GetOperationQueueMemoryUse, size_t());
    MOCK_CONST_METHOD0(GetIncIndexMemoryUse, size_t());

    MOCK_CONST_METHOD4(EstimateDiffVersionLockSize, size_t(
                    const config::IndexPartitionSchemaPtr& schema,
                    const file_system::DirectoryPtr& directory,
                    const index_base::Version& version,
                    const index_base::Version& lastLoadedVersion));
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MOCK_PARTITION_RESOURCE_CALCULATOR_H
