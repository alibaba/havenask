#ifndef ISEARCH_BS_BUILDER_H
#define ISEARCH_BS_BUILDER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/document/document.h>
#include <indexlib/partition/index_builder.h>
#include "build_service/config/BuilderConfig.h"
#include "build_service/builder/BuilderMetrics.h"
#include "build_service/builder/BuildSpeedLimiter.h"
#include "build_service/common/Locator.h"
#include "build_service/proto/ErrorCollector.h"
#include <autil/Lock.h>
#include <fslib/fs/FileLock.h>

DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);
DECLARE_REFERENCE_CLASS(util, StateCounter);

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
IE_NAMESPACE_END(misc);

namespace build_service {
namespace builder {

class Builder : public proto::ErrorCollector
{
public:
    enum RET_STAT
    {
        RS_LOCK_BUSY,
        RS_OK,
        RS_FAIL,
    };
    
public:
    Builder(const IE_NAMESPACE(partition)::IndexBuilderPtr &indexBuilder,
            fslib::fs::FileLock *fileLock = NULL,
            const proto::BuildId &buildId = proto::BuildId());
    virtual ~Builder();
private:
    Builder(const Builder &);
    Builder& operator=(const Builder &);
public:
    virtual bool init(const config::BuilderConfig &builderConfig,
                      IE_NAMESPACE(misc)::MetricProviderPtr metricProvider = 
                      IE_NAMESPACE(misc)::MetricProviderPtr());
    // only return false when exception
    virtual bool build(const IE_NAMESPACE(document)::DocumentPtr &doc);
    virtual bool merge(const IE_NAMESPACE(config)::IndexPartitionOptions &options);
    virtual bool tryDump();
    virtual void stop(int64_t stopTimestamp = INVALID_TIMESTAMP);
    virtual int64_t getIncVersionTimestamp() const; // for realtime build
    virtual bool getLastLocator(common::Locator &locator) const;
    virtual bool getLatestVersionLocator(common::Locator &locator) const;
    virtual bool getLastFlushedLocator(common::Locator &locator);

    // virtual for test
    virtual bool hasFatalError() const;
    virtual const IE_NAMESPACE(util)::CounterMapPtr& GetCounterMap()
    {
        static IE_NAMESPACE(util)::CounterMapPtr nullCounterMap = IE_NAMESPACE(util)::CounterMapPtr();
        return _indexBuilder ? _indexBuilder->GetCounterMap() : nullCounterMap;
    }

    void enableSpeedLimiter() { _speedLimiter.enableLimiter(); }

public:
    virtual RET_STAT getIncVersionTimestampNonBlocking(int64_t& ts) const; // for realtime build
    virtual RET_STAT getLastLocatorNonBlocking(common::Locator &locator) const;

public:
    proto::BuildStep TEST_GET_BUILD_STEP();//for test

protected:
    // virtual for test
    virtual bool doBuild(const IE_NAMESPACE(document)::DocumentPtr &doc);
    virtual void doMerge(const IE_NAMESPACE(config)::IndexPartitionOptions &options);
    virtual void setFatalError();
    std::string docOperateTypeToStr(int32_t type);
    
protected:
    mutable autil::ThreadMutex _indexBuilderMutex;
    IE_NAMESPACE(partition)::IndexBuilderPtr _indexBuilder;
    fslib::fs::FileLock *_fileLock; // hold this lock
    BuilderMetrics _builderMetrics;
    IE_NAMESPACE(util)::AccumulativeCounterPtr _totalDocCountCounter;
    IE_NAMESPACE(util)::StateCounterPtr _builderDocCountCounter;
    proto::BuildId _buildId;
    
private:
    volatile bool _fatalError;
    std::string _lastPk;
    BuildSpeedLimiter _speedLimiter;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(Builder);

}
}

#endif //ISEARCH_BS_BUILDER_H
