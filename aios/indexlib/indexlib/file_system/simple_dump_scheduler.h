#ifndef __INDEXLIB_SIMPLE_DUMP_SCHEDULER_H
#define __INDEXLIB_SIMPLE_DUMP_SCHEDULER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/dump_scheduler.h"

IE_NAMESPACE_BEGIN(file_system);

class SimpleDumpScheduler : public DumpScheduler
{
public:
    SimpleDumpScheduler();
    ~SimpleDumpScheduler();
public:
    void Push(const DumpablePtr& dumpTask) override;
    void WaitFinished() override;
    bool HasDumpTask() override { return false; }
    void WaitDumpQueueEmpty() override {}

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SimpleDumpScheduler);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SIMPLE_DUMP_SCHEDULER_H
