#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/open_executor/open_executor_chain.h"

namespace indexlib { namespace partition {

class FakeOpenExecutorChain : public OpenExecutorChain
{
public:
    FakeOpenExecutorChain();
    ~FakeOpenExecutorChain();

public:
    void SetExecutorFalseIdx(int32_t idx) { mExecutorFalseIdx = idx; }
    void SetDropExeceptionIdx(int32_t idx) { mDropExeceptionIdx = idx; }
    void SetExecutorExceptionIdx(int32_t idx) { mExecutorExceptionIdx = idx; }
    void DropOne(int32_t failId, ExecutorResource& resource) override;
    bool ExecuteOne(int32_t idx, ExecutorResource& resource) override;

private:
    int32_t mExecutorFalseIdx;
    int32_t mDropExeceptionIdx;
    int32_t mExecutorExceptionIdx;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeOpenExecutorChain);
}} // namespace indexlib::partition
