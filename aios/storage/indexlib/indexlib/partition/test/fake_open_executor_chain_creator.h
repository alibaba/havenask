#ifndef __INDEXLIB_FAKE_EXECUTOR_CHAIN_CREATOR_H
#define __INDEXLIB_FAKE_EXECUTOR_CHAIN_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/open_executor/open_executor_chain_creator.h"
#include "indexlib/partition/test/fake_open_executor_chain.h"

namespace indexlib { namespace partition {
class FakeOpenExecutorChainCreator : public OpenExecutorChainCreator
{
public:
    FakeOpenExecutorChainCreator(std::string partitionName, OnlinePartition* partition)
        : OpenExecutorChainCreator(partitionName, partition)
    {
    }

    // for test
    ~FakeOpenExecutorChainCreator() {}

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
DEFINE_SHARED_PTR(FakeOpenExecutorChainCreator);
}} // namespace indexlib::partition

#endif //__INDEXLIB_FAKE_EXECUTOR_CHAIN_CREATOR_H
