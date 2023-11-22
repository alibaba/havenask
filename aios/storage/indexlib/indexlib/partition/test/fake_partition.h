#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/test/fake_open_executor_chain.h"
#include "indexlib/partition/test/fake_open_executor_chain_creator.h"

namespace indexlib { namespace partition {
class FakePartition : public OnlinePartition
{
public:
    FakePartition(const std::string& partitionName, const IndexPartitionResource& partitionResource)
        : OnlinePartition(partitionResource)
        , mCreator(new FakeOpenExecutorChainCreator(partitionName, this))
    {
    }

    // FakePartition(const std::string& partitionName, const util::MemoryQuotaControllerPtr& memQuotaController,
    //               util::MetricProviderPtr metricProvider = util::MetricProviderPtr())
    //     : OnlinePartition(partitionName, memQuotaController, metricProvider)
    //     , mCreator(new FakeOpenExecutorChainCreator(partitionName, this))
    // {
    // }

    // for test
    ~FakePartition() {}

public:
    OpenExecutorChainCreatorPtr CreateChainCreator() override
    {
        return DYNAMIC_POINTER_CAST(OpenExecutorChainCreator, mCreator);
    }

public:
    FakeOpenExecutorChainCreatorPtr mCreator;
};
DEFINE_SHARED_PTR(FakePartition);
}} // namespace indexlib::partition
