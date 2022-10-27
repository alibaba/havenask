#ifndef __INDEXLIB_FAKE_OPEN_EXECUTOR_CHAIN_H
#define __INDEXLIB_FAKE_OPEN_EXECUTOR_CHAIN_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/open_executor_chain.h"

IE_NAMESPACE_BEGIN(partition);

class FakeOpenExecutorChain : public  OpenExecutorChain
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

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_FAKE_OPEN_EXECUTOR_CHAIN_H
