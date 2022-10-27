#ifndef ISEARCH_BS_ASYNCBUILDER_H
#define ISEARCH_BS_ASYNCBUILDER_H
#include <autil/Thread.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/util/StreamQueue.h"
#include "build_service/util/MemControlStreamQueue.h"
#include "build_service/builder/Builder.h"

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
IE_NAMESPACE_END(misc);

namespace build_service {
namespace builder {

class AsyncBuilder : public Builder
{
public:
    AsyncBuilder(const IE_NAMESPACE(partition)::IndexBuilderPtr &indexBuilder,
                 fslib::fs::FileLock *fileLock = NULL,
                 const proto::BuildId &buildId = proto::BuildId());
    /*override*/ ~AsyncBuilder();
private:
    AsyncBuilder(const AsyncBuilder &);
    AsyncBuilder& operator=(const AsyncBuilder &);

public:
    /*override*/ bool init(const config::BuilderConfig &builderConfig,
                      IE_NAMESPACE(misc)::MetricProviderPtr metricProvider =  
                           IE_NAMESPACE(misc)::MetricProviderPtr());
    // only return false when exception
    /*override*/ bool build(const IE_NAMESPACE(document)::DocumentPtr &doc);
    /*override*/ bool merge(const IE_NAMESPACE(config)::IndexPartitionOptions &options);
    /*override*/ void stop(int64_t stopTimestamp = INVALID_TIMESTAMP);
    /*override*/ bool isFinished() const 
    { return !_running || _docQueue->empty(); }

private:
    void buildThread();
    void releaseDocs();
    void clearQueue();

private:
    typedef util::MemControlStreamQueue<IE_NAMESPACE(document)::DocumentPtr> DocQueue;
    typedef util::StreamQueue<IE_NAMESPACE(document)::DocumentPtr> Queue;
    typedef std::unique_ptr<DocQueue> DocQueuePtr;    
    typedef std::unique_ptr<Queue> QueuePtr;
    
    volatile bool _running;
    autil::ThreadPtr _asyncBuildThreadPtr;
    DocQueuePtr _docQueue;
    QueuePtr _releaseDocQueue;

    IE_NAMESPACE(misc)::MetricPtr _asyncQueueSizeMetric;
    IE_NAMESPACE(misc)::MetricPtr _asyncQueueMemMetric;

private:
    friend class AsyncBuilderTest;
    BS_LOG_DECLARE();

};

BS_TYPEDEF_PTR(AsyncBuilder);

}
}

#endif //ISEARCH_BS_ASYNCBUILDER_H
