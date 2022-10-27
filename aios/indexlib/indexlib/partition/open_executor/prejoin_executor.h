#ifndef __INDEXLIB_PREJOIN_EXECUTOR_H
#define __INDEXLIB_PREJOIN_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/join_segment_writer.h"
#include "indexlib/partition/open_executor/open_executor.h"


IE_NAMESPACE_BEGIN(partition);

class PrejoinExecutor : public OpenExecutor
{
public:
    PrejoinExecutor(const JoinSegmentWriterPtr& joinSegWriter)
        : mJoinSegWriter(joinSegWriter)
    { }
    ~PrejoinExecutor() { }

    bool Execute(ExecutorResource& resource) override;

    void Drop(ExecutorResource& resource) override 
    {
        mJoinSegWriter.reset();
    }

private:
    JoinSegmentWriterPtr mJoinSegWriter;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrejoinExecutor);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PREJOIN_EXECUTOR_H
