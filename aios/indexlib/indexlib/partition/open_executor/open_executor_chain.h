#ifndef __INDEXLIB_OPEN_EXECUTOR_CHAIN_H
#define __INDEXLIB_OPEN_EXECUTOR_CHAIN_H

#include <tr1/memory>
#include <vector>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/open_executor.h"

IE_NAMESPACE_BEGIN(partition);

class OpenExecutorChain
{
public:
    OpenExecutorChain();
    ~OpenExecutorChain();
public:
    bool Execute(ExecutorResource& resource);
    void PushBack(const OpenExecutorPtr& executor);
protected:
    void Drop(int32_t failId, ExecutorResource& resource);
    virtual void DropOne(int32_t idx, ExecutorResource& resource) 
    {
        mExecutors[idx]->Drop(resource);
    }
    virtual bool ExecuteOne(int32_t idx, ExecutorResource& resource) 
    {
        return mExecutors[idx]->Execute(resource);
    }
private:
    typedef std::vector<OpenExecutorPtr> OpenExecutorVec;
    OpenExecutorVec mExecutors;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OpenExecutorChain);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPEN_EXECUTOR_CHAIN_H
