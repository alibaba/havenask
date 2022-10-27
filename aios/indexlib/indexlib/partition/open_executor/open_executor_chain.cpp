#include "indexlib/partition/open_executor/open_executor_chain.h"
#include "indexlib/misc/exception.h"
#include <memory>

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OpenExecutorChain);

OpenExecutorChain::OpenExecutorChain() 
{
}

OpenExecutorChain::~OpenExecutorChain() 
{
}

bool OpenExecutorChain::Execute(ExecutorResource& resource)
{
    int32_t idx = 0;
    try
    {
        for (; idx < (int32_t)mExecutors.size(); ++idx)
        {
            assert(mExecutors[idx]);
            if (!ExecuteOne(idx, resource))
            {
                Drop(idx, resource);
                return false;
            }
        }
    }
    catch (const FileIOException& e)
    {
        resource.mNeedReload.store(true, std::memory_order_relaxed);
        //Drop(idx, resource);
        throw e;
    }
    catch (const ExceptionBase& e)
    {
        resource.mNeedReload.store(true, std::memory_order_relaxed);
        //Drop(idx, resource);
        throw e;
    }
    catch (const exception& e)
    {
        resource.mNeedReload.store(true, std::memory_order_relaxed);
        //Drop(idx, resource);
        throw e;
    }

    return true;
}

void OpenExecutorChain::Drop(int32_t failId, ExecutorResource& resource)
{
    IE_LOG(WARN, "open/reopen failed, fail id [%d], executor drop begin", failId);
    for (int i = failId; i >= 0; --i)
    {
        assert(mExecutors[i]);
        DropOne(i, resource);
    }
    IE_LOG(WARN, "executor drop end");
}

void OpenExecutorChain::PushBack(const OpenExecutorPtr& executor)
{
    mExecutors.push_back(executor);
}



IE_NAMESPACE_END(partition);

