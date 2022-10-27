#ifndef __INDEXLIB_PRELOAD_EXECUTOR_H
#define __INDEXLIB_PRELOAD_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/open_executor.h"

DECLARE_REFERENCE_CLASS(partition, OnlinePartitionReader);
DECLARE_REFERENCE_CLASS(util, MemoryReserver);

IE_NAMESPACE_BEGIN(partition);

class PreloadExecutor : public OpenExecutor
{
public:
    PreloadExecutor();
    ~PreloadExecutor();

    bool Execute(ExecutorResource& resource) override;
    void Drop(ExecutorResource& resource) override;

private:
    bool ReserveMem(ExecutorResource& resource, const util::MemoryReserverPtr& memReserver);

private:
    partition::OnlinePartitionReaderPtr mPreloadIncReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PreloadExecutor);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PRELOAD_EXECUTOR_H
