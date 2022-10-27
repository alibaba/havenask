#ifndef __INDEXLIB_OPEN_EXECUTOR_H
#define __INDEXLIB_OPEN_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/executor_resource.h"

IE_NAMESPACE_BEGIN(partition);

class OpenExecutor
{
public:
    OpenExecutor(const std::string& partitionName = "")
        : mPartitionName(partitionName)
    {}
    virtual ~OpenExecutor() {}

public:
    virtual bool Execute(ExecutorResource& resource) = 0;
    virtual void Drop(ExecutorResource& resource) = 0;

protected:
    std::string mPartitionName;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OpenExecutor);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPEN_EXECUTOR_H
