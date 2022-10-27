#include "indexlib/file_system/simple_dump_scheduler.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SimpleDumpScheduler);

SimpleDumpScheduler::SimpleDumpScheduler() 
{
}

SimpleDumpScheduler::~SimpleDumpScheduler() 
{
}

void SimpleDumpScheduler::Push(const DumpablePtr& dumpTask)
{
    dumpTask->Dump();
}

void SimpleDumpScheduler::WaitFinished()
{
    return;
}
IE_NAMESPACE_END(file_system);

