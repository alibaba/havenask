#ifndef ISEARCH_BS_PROCESSEDDOCRTBUILDERIMPL_H
#define ISEARCH_BS_PROCESSEDDOCRTBUILDERIMPL_H

#include "build_service/workflow/RealtimeBuilderImplBase.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Workflow.h"
#include <autil/Lock.h>
#include <autil/LoopThread.h>

namespace build_service {

namespace builder {
class Builder;
}

namespace workflow {

class BrokerFactory;
class SwiftProcessedDocProducer;

class ProcessedDocRtBuilderImpl : public RealtimeBuilderImplBase
{
public:
    ProcessedDocRtBuilderImpl(const std::string &configPath,
                              const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart,
                              const RealtimeBuilderResource& builderResource);
    virtual ~ProcessedDocRtBuilderImpl();
private:
    ProcessedDocRtBuilderImpl(const ProcessedDocRtBuilderImpl &);
    ProcessedDocRtBuilderImpl& operator=(const ProcessedDocRtBuilderImpl &);
protected: /*override*/
    bool doStart(const proto::PartitionId &partitionId, KeyValueMap &kvMap);
    bool getLastTimestampInProducer(int64_t &timestamp) override;
    bool getLastReadTimestampInProducer(int64_t &timestamp) override;
    bool producerAndBuilderPrepared() const;
    bool producerSeek(const common::Locator &locator);
    void externalActions();
private:
    bool getBuilderAndProducer();
    void skipToLatestLocator();
    void reportFreshnessWhenSuspendBuild();
    void resetStartSkipTimestamp(KeyValueMap &kvMap);
    
protected:
    //virtual for test
    virtual BrokerFactory *createBrokerFactory() const;
    virtual int64_t getStartSkipTimestamp() const;
    
private:
    std::unique_ptr<BrokerFactory> _brokerFactory;
    SwiftProcessedDocProducer *_producer;
    bool _startSkipCalibrateDone;    // for flushed realtime index, need skip to rt locator
    common::Locator _lastSkipedLocator;
    
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_PROCESSEDDOCRTBUILDERIMPL_H
