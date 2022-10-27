#ifndef ISEARCH_BS_BROKERFACTORY_H
#define ISEARCH_BS_BROKERFACTORY_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/Consumer.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/processor/Processor.h"
#include "build_service/builder/Builder.h"
#include "build_service/proto/ErrorCollector.h"
#include <indexlib/config/index_partition_schema.h>

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr; 
IE_NAMESPACE_END(misc);

namespace build_service {
namespace common {
    class CounterSynchronizer;
    BS_TYPEDEF_PTR(CounterSynchronizer);
}

namespace workflow {

class BrokerFactory : public proto::ErrorCollector
{
public:
    struct RoleInitParam {
        RoleInitParam()
            : metricProvider(NULL)
            , specifiedTopicId(-1)
            , isReadToBuildFlow(false)
        {}
        config::ResourceReaderPtr resourceReader;
        KeyValueMap kvMap;
        proto::PartitionId partitionId;
        IE_NAMESPACE(misc)::MetricProviderPtr metricProvider;
        uint8_t swiftFilterMask;
        uint8_t swiftFilterResult;
        IE_NAMESPACE(util)::CounterMapPtr counterMap;
        common::CounterSynchronizerPtr counterSynchronizer;
        int64_t specifiedTopicId;
        IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema;
        bool isReadToBuildFlow;
    };
public:
    BrokerFactory() {
        setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
    }
    virtual ~BrokerFactory() {}
private:
    BrokerFactory(const BrokerFactory &);
    BrokerFactory& operator=(const BrokerFactory &);
public:
    virtual RawDocProducer *createRawDocProducer(
            const RoleInitParam &initParam) = 0;
    virtual RawDocConsumer *createRawDocConsumer(
            const RoleInitParam &initParam) = 0;

    virtual ProcessedDocProducer *createProcessedDocProducer(
            const RoleInitParam &initParam) = 0;
    virtual ProcessedDocConsumer *createProcessedDocConsumer(
            const RoleInitParam &initParam) = 0;

    virtual bool initCounterMap(RoleInitParam &initParam) = 0;
    virtual void collectErrorInfos(std::vector<proto::ErrorInfo> &errorInfos) const
    {
        fillErrorInfos(errorInfos);
    }
public:
    virtual void stopProduceProcessedDoc() {
    }
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BrokerFactory);

}
}

#endif //ISEARCH_BS_BROKERFACTORY_H
