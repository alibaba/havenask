#ifndef __INDEXLIB_DUMP_SCHEDULER_H
#define __INDEXLIB_DUMP_SCHEDULER_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/file_system/dumpable.h"

IE_NAMESPACE_BEGIN(file_system);

class DumpScheduler
{
public:
    DumpScheduler() {}
    virtual ~DumpScheduler() {}
public:
    virtual void Push(const DumpablePtr& dumpTask) = 0;
    virtual void WaitFinished() = 0;
    virtual bool HasDumpTask() = 0;
    virtual void WaitDumpQueueEmpty() = 0;
};

DEFINE_SHARED_PTR(DumpScheduler);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_DUMP_SCHEDULER_H
