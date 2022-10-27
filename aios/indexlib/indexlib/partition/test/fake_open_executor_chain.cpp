#include "indexlib/partition/test/fake_open_executor_chain.h"

using namespace std;

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, FakeOpenExecutorChain);

FakeOpenExecutorChain::FakeOpenExecutorChain() 
    : mExecutorFalseIdx(-1)
    , mDropExeceptionIdx(-1)
    , mExecutorExceptionIdx(-1)
{
}

FakeOpenExecutorChain::~FakeOpenExecutorChain() 
{
}

void FakeOpenExecutorChain::DropOne(int32_t idx, ExecutorResource& resource)
{
    OpenExecutorChain::DropOne(idx, resource);
    if (idx == mDropExeceptionIdx) 
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "fake drop exception");
    }
}

bool FakeOpenExecutorChain::ExecuteOne(
        int32_t idx, ExecutorResource& resource) 
{
    bool ret = OpenExecutorChain::ExecuteOne(idx, resource);
    if (!ret)
    {
        return ret;
    }
    if (idx == mExecutorFalseIdx) 
    {
        return false;
    }
    if (idx == mExecutorExceptionIdx) 
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "fake drop exception");
    }
    return true;
}

IE_NAMESPACE_END(partition);

