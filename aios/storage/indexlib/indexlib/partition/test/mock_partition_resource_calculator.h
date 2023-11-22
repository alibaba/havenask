#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/partition_resource_calculator.h"

namespace indexlib { namespace partition {

class MockPartitionResourceCalculator : public PartitionResourceCalculator
{
public:
    MockPartitionResourceCalculator(const config::OnlineConfig& onlineConfig)
        : PartitionResourceCalculator(onlineConfig)
    {
    }

    ~MockPartitionResourceCalculator() {}

public:
    MOCK_METHOD(size_t, GetCurrentMemoryUse, (), (const, override));
    MOCK_METHOD(size_t, GetRtIndexMemoryUse, (), (const, override));
    MOCK_METHOD(size_t, GetWriterMemoryUse, (), (const, override));
    MOCK_METHOD(size_t, GetOperationQueueMemoryUse, (), (const));
    MOCK_METHOD(size_t, GetIncIndexMemoryUse, (), (const, override));
    MOCK_METHOD(size_t, EstimateDiffVersionLockSize,
                (const config::IndexPartitionSchemaPtr& schema, const file_system::DirectoryPtr& directory,
                 const index_base::Version& version, const index_base::Version& lastLoadedVersion),
                (const));
};
}} // namespace indexlib::partition
