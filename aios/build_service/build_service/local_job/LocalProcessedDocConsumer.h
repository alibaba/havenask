#ifndef ISEARCH_BS_LOCALPROCESSEDDOCCONSUMER_H
#define ISEARCH_BS_LOCALPROCESSEDDOCCONSUMER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Consumer.h"
#include "build_service/local_job/ReduceDocumentQueue.h"

namespace build_service {
namespace local_job {

class LocalProcessedDocConsumer : public workflow::ProcessedDocConsumer
{
public:
    LocalProcessedDocConsumer(ReduceDocumentQueue *queue,
                              const std::vector<int32_t> &reduceIdxTable);
    ~LocalProcessedDocConsumer();
private:
    LocalProcessedDocConsumer(const LocalProcessedDocConsumer &);
    LocalProcessedDocConsumer& operator=(const LocalProcessedDocConsumer &);
public:
    /* override */ workflow::FlowError consume(const document::ProcessedDocumentVecPtr &document);
    /* override */ bool getLocator(common::Locator &locator) const;
    /* override */ bool stop(workflow::FlowError lastError);
private:
    ReduceDocumentQueue *_queue;
    std::vector<int32_t> _reduceIdxTable;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_LOCALPROCESSEDDOCCONSUMER_H
