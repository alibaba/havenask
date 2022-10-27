#ifndef ISEARCH_BS_REALTIMEBUILDERDEFINE_H
#define ISEARCH_BS_REALTIMEBUILDERDEFINE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/WorkflowThreadPool.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ProtoComparator.h"
#include <indexlib/util/task_scheduler.h>
#include <indexlib/misc/metric_provider.h>

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
IE_NAMESPACE_END(misc);

namespace build_service {

namespace util {
class SwiftClientCreator;
typedef std::tr1::shared_ptr<SwiftClientCreator> SwiftClientCreatorPtr;
}

namespace workflow {

struct BuildFlowThreadResource
{
public:
    WorkflowThreadPoolPtr readerToProcessorThreadPool;
    WorkflowThreadPoolPtr processorToBuilderThreadPool;

public:
    BuildFlowThreadResource(
            const WorkflowThreadPoolPtr& threadPool = WorkflowThreadPoolPtr())
        : readerToProcessorThreadPool(threadPool)
        , processorToBuilderThreadPool(threadPool)
    {}

    BuildFlowThreadResource(const WorkflowThreadPoolPtr& r2pThreadPool,
                            const WorkflowThreadPoolPtr& p2bThreadPool)
        : readerToProcessorThreadPool(r2pThreadPool)
        , processorToBuilderThreadPool(p2bThreadPool)
    {}
};

struct RealtimeBuilderResource
{
public:
    IE_NAMESPACE(misc)::MetricProviderPtr metricProvider;
    IE_NAMESPACE(util)::TaskSchedulerPtr taskScheduler;
    util::SwiftClientCreatorPtr swiftClientCreator;
    BuildFlowThreadResource buildFlowThreadResource;

public:
    RealtimeBuilderResource(
        kmonitor::MetricsReporterPtr reporter_,
        IE_NAMESPACE(util)::TaskSchedulerPtr taskScheduler_,
        util::SwiftClientCreatorPtr swiftClientCreator_,
        const BuildFlowThreadResource& buildFlowThreadResource_ = BuildFlowThreadResource())
        : taskScheduler(taskScheduler_)
        , swiftClientCreator(swiftClientCreator_)
        , buildFlowThreadResource(buildFlowThreadResource_)
    {
        metricProvider.reset(new IE_NAMESPACE(misc)::MetricProvider(reporter_));
    }

    RealtimeBuilderResource(
        IE_NAMESPACE(misc)::MetricProviderPtr metricProvider_,
        IE_NAMESPACE(util)::TaskSchedulerPtr taskScheduler_,
        util::SwiftClientCreatorPtr swiftClientCreator_,
        const BuildFlowThreadResource& buildFlowThreadResource_ = BuildFlowThreadResource())
        : metricProvider(metricProvider_)
        , taskScheduler(taskScheduler_)
        , swiftClientCreator(swiftClientCreator_)
        , buildFlowThreadResource(buildFlowThreadResource_)
    {
    }
};


}
}

#endif //ISEARCH_BS_REALTIMEBUILDERDEFINE_H
