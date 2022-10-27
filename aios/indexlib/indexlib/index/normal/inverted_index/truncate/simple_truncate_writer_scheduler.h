#ifndef __INDEXLIB_SIMPLE_TRUNCATE_WRITER_SCHEDULER_H
#define __INDEXLIB_SIMPLE_TRUNCATE_WRITER_SCHEDULER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_writer_scheduler.h"

IE_NAMESPACE_BEGIN(index);

class SimpleTruncateWriterScheduler : public TruncateWriterScheduler
{
public:
    SimpleTruncateWriterScheduler();
    ~SimpleTruncateWriterScheduler();

public:
    /* override */ void PushWorkItem(autil::WorkItem* workItem);
    /* override */ void WaitFinished();

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SimpleTruncateWriterScheduler);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SIMPLE_TRUNCATE_WRITER_SCHEDULER_H
