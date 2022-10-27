#ifndef ISEARCH_BS_LOCALBROKERFACTORY_H
#define ISEARCH_BS_LOCALBROKERFACTORY_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/BrokerFactory.h"
#include "build_service/local_job/ReduceDocumentQueue.h"
#include <indexlib/util/counter/counter_map.h>

namespace build_service {
namespace local_job {

class LocalBrokerFactory : public workflow::BrokerFactory
{
public:
    LocalBrokerFactory(ReduceDocumentQueue *queue);
    ~LocalBrokerFactory();
private:
    LocalBrokerFactory(const LocalBrokerFactory &);
    LocalBrokerFactory& operator=(const LocalBrokerFactory &);
public:
    /* override */ workflow::RawDocProducer *createRawDocProducer(
            const RoleInitParam &initParam);
    /* override */ workflow::RawDocConsumer *createRawDocConsumer(
            const RoleInitParam &initParam);

    /* override */ workflow::ProcessedDocProducer *createProcessedDocProducer(
            const RoleInitParam &initParam);
    /* override */ workflow::ProcessedDocConsumer *createProcessedDocConsumer(
            const RoleInitParam &initParam);
    bool initCounterMap(RoleInitParam &initParam) override {
        initParam.counterMap.reset(new IE_NAMESPACE(util)::CounterMap());
        return true;
    }

private:
    ReduceDocumentQueue *_queue;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_LOCALBROKERFACTORY_H
