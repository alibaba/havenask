#ifndef ISEARCH_BS_SLEEPWORKITEM_H
#define ISEARCH_BS_SLEEPWORKITEM_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/ProcessorWorkItem.h"
#include <autil/AtomicCounter.h>
#include <autil/TimeUtility.h>

namespace build_service {
namespace processor {

class SleepWorkItem : public ProcessorWorkItem {
public:
    SleepWorkItem(uint32_t sleepTime, autil::AtomicCounter& processedCount)
        : ProcessorWorkItem(DocumentProcessorChainVecPtr(), ProcessorChainSelectorPtr(), NULL)
        , _sleepTime(sleepTime)
        , _processedCount(processedCount)
    {
    }
    ~SleepWorkItem() {
    }
public:
    /* override */ void process() {
        sleep(_sleepTime);
        _processedCount.inc();
        _processedDocumentVecPtr.reset(new document::ProcessedDocumentVec);
    }

private:
    uint32_t _sleepTime;
    autil::AtomicCounter &_processedCount;
};

}
}

#endif //ISEARCH_BS_SLEEPWORKITEM_H
