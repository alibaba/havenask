#include "indexlib/index/normal/inverted_index/truncate/simple_truncate_writer_scheduler.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SimpleTruncateWriterScheduler);

SimpleTruncateWriterScheduler::SimpleTruncateWriterScheduler() 
{
}

SimpleTruncateWriterScheduler::~SimpleTruncateWriterScheduler() 
{
}
 
void SimpleTruncateWriterScheduler::PushWorkItem(autil::WorkItem* workItem)
{
    try
    {
        workItem->process();
    }
    catch (...)
    {
        workItem->destroy();        
        throw;
    }
    workItem->destroy();
}

void SimpleTruncateWriterScheduler::WaitFinished()
{
    
}

IE_NAMESPACE_END(index);

