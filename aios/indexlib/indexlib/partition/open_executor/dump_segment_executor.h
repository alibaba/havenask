#ifndef __INDEXLIB_DUMP_SEGMENT_EXECUTOR_H
#define __INDEXLIB_DUMP_SEGMENT_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/open_executor.h"

IE_NAMESPACE_BEGIN(partition);

class DumpSegmentExecutor : public OpenExecutor
{
public:
    DumpSegmentExecutor(const std::string& partitionName = "",
                        bool reInitReaderAndWriter = true);
    ~DumpSegmentExecutor();

public:
    bool Execute(ExecutorResource& resource) override;
    void Drop(ExecutorResource& resource);

private:
    bool mReInitReaderAndWriter;
    bool mHasSkipInitReaderAndWriter;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DumpSegmentExecutor);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_DUMP_SEGMENT_EXECUTOR_H
