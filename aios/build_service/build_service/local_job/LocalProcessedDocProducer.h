#ifndef ISEARCH_BS_LOCALPROCESSEDDOCPRODUCER_H
#define ISEARCH_BS_LOCALPROCESSEDDOCPRODUCER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Producer.h"
#include "build_service/local_job/ReduceDocumentQueue.h"

namespace build_service {
namespace local_job {

class LocalProcessedDocProducer : public workflow::ProcessedDocProducer
{
public:
    LocalProcessedDocProducer(ReduceDocumentQueue *queue,
                              uint64_t src,
                              uint16_t instanceId);
    ~LocalProcessedDocProducer();
private:
    LocalProcessedDocProducer(const LocalProcessedDocProducer &);
    LocalProcessedDocProducer& operator=(const LocalProcessedDocProducer &);
public:
    /* override */ workflow::FlowError produce(document::ProcessedDocumentVecPtr &processedDocVec);
    /* override */ bool seek(const common::Locator &locator);
    /* override */ bool stop();
private:
    ReduceDocumentQueue *_queue;
    uint64_t _src;
    uint16_t _instanceId;
    uint32_t _curDocCount;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(LocalProcessedDocProducer);

}
}

#endif //ISEARCH_BS_LOCALPROCESSEDDOCPRODUCER_H
