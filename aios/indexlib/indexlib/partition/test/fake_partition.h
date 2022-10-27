#ifndef __INDEXLIB_FAKE_PARTITION_H
#define __INDEXLIB_FAKE_PARTITION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/test/fake_open_executor_chain.h"

IE_NAMESPACE_BEGIN(partition);
class FakePartition : public OnlinePartition
{
public:
    FakePartition(const std::string& partitionName,
                  const IndexPartitionResource& partitionResource)
        : OnlinePartition(partitionName, partitionResource) {}

    FakePartition(const std::string& partitionName,
                  const util::MemoryQuotaControllerPtr& memQuotaController,
                  misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr())
        : OnlinePartition(partitionName, memQuotaController, metricProvider) {}
    
    //for test
    ~FakePartition() {}
public:
    void SetExecutorFalseIdx(int32_t idx) { mExecutorFalseIdx = idx; }
    void SetDropExeceptionIdx(int32_t idx) { mDropExeceptionIdx = idx; }
    void SetExecutorExceptionIdx(int32_t idx) { mExecutorExceptionIdx = idx; }
    OpenExecutorChainPtr createOpenExecutorChain() override
    {
        FakeOpenExecutorChainPtr chain(new FakeOpenExecutorChain);
        chain->SetExecutorFalseIdx(mExecutorFalseIdx);
        chain->SetDropExeceptionIdx(mDropExeceptionIdx);
        chain->SetExecutorExceptionIdx(mExecutorExceptionIdx);
        return chain;
    }
private:
    int32_t mExecutorFalseIdx;
    int32_t mDropExeceptionIdx;
    int32_t mExecutorExceptionIdx;
};
DEFINE_SHARED_PTR(FakePartition);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_FAKE_PARTITION_H
