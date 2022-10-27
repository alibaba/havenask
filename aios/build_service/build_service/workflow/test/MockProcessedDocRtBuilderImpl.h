#ifndef ISEARCH_BS_MOCKREALTIMEBUILDERIMPL_H
#define ISEARCH_BS_MOCKREALTIMEBUILDERIMPL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/ProcessedDocRtBuilderImpl.h"
#include "build_service/util/SwiftClientCreator.h"
#include <indexlib/misc/metric_provider.h>

namespace build_service {
namespace workflow {

class MockProcessedDocRtBuilderImpl : public ProcessedDocRtBuilderImpl
{
public:
    MockProcessedDocRtBuilderImpl(const std::string &configPath,
                                  const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart,
                                  const RealtimeBuilderResource& builderResource =
                                  RealtimeBuilderResource(
                                      kmonitor::MetricsReporterPtr(),
                                      IE_NAMESPACE(util)::TaskSchedulerPtr(new IE_NAMESPACE(util)::TaskScheduler),
                                      util::SwiftClientCreatorPtr(new util::SwiftClientCreator)))
    : ProcessedDocRtBuilderImpl(configPath, indexPart, builderResource)
    {
        ON_CALL(*this, seekProducerToLatest()).WillByDefault(Throw(std::runtime_error("uninteresting call")));
    }
    ~MockProcessedDocRtBuilderImpl() {}

public:
    void lockRealtimeMutex()
    {
        autil::ScopedLock lock(_realtimeMutex);
        usleep(1000000);
    }
        
protected:
    MOCK_CONST_METHOD0(createBrokerFactory, BrokerFactory*());
    MOCK_CONST_METHOD2(createBuildFlow,
                       BuildFlow*(const IE_NAMESPACE(partition)::IndexPartitionPtr&,
                                  const util::SwiftClientCreatorPtr &swiftClientCreator));
    MOCK_CONST_METHOD0(getIndexRoot, std::string());
    MOCK_CONST_METHOD0(getStartSkipTimestamp, int64_t());
    MOCK_METHOD0(seekProducerToLatest, bool());

private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_MOCKREALTIMEBUILDERIMPL_H
