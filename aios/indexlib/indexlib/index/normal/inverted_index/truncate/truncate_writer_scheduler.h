#ifndef __INDEXLIB_TRUNCATE_WRITER_SCHEDULER_H
#define __INDEXLIB_TRUNCATE_WRITER_SCHEDULER_H

#include <tr1/memory>
#include <autil/WorkItem.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

class TruncateWriterScheduler
{
public:
    TruncateWriterScheduler() {};
    virtual ~TruncateWriterScheduler() {};

public:
    virtual void PushWorkItem(autil::WorkItem* workItem) = 0;
    virtual void WaitFinished() = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateWriterScheduler);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNCATE_WRITER_SCHEDULER_H
