#ifndef ISEARCH_BS_REALTIMEBUILDERIMPLBASE_H
#define ISEARCH_BS_REALTIMEBUILDERIMPLBASE_H

#include <indexlib/partition/index_partition.h>
#include <indexlib/util/task_scheduler.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/workflow/RealtimeErrorDefine.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/AsyncStarter.h"
#include <indexlib/partition/index_partition.h>

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
IE_NAMESPACE_END(misc);

namespace build_service {
namespace workflow {

class RealtimeBuilderImplBase
{
public:
    RealtimeBuilderImplBase(
            const std::string &configPath,
            const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart,
            const RealtimeBuilderResource& builderResource,
            bool supportForceSeek);

    virtual ~RealtimeBuilderImplBase();
    
private:
    RealtimeBuilderImplBase(const RealtimeBuilderImplBase &);
    RealtimeBuilderImplBase& operator=(const RealtimeBuilderImplBase &);
public:
    bool start(const proto::PartitionId &partitionId, KeyValueMap &kvMap);
    void stop();
    void suspendBuild();
    void resumeBuild();
    void forceRecover();
    void setTimestampToSkip(int64_t timestamp);
    bool isRecovered() const;
    void getErrorInfo(RealtimeErrorCode &errorCode,
                      std::string &errorMsg,
                      int64_t &errorTime) const;
    void executeBuildControlTask();
    builder::Builder* GetBuilder() const { return _builder; } 
    
protected:
    virtual bool doStart(const proto::PartitionId &partitionId, KeyValueMap &kvMap) = 0;
protected:
    bool prepareIntervalTask(const config::ResourceReaderPtr &resourceReader,
                             const proto::PartitionId &partitionId,
                             IE_NAMESPACE(misc)::MetricProviderPtr metricProvider);
    
    void setErrorInfoUnsafe(RealtimeErrorCode errorCode, const std::string &errorMsg);
protected:
    virtual bool producerAndBuilderPrepared() const = 0;
    virtual void externalActions() = 0;
    virtual bool getLastTimestampInProducer(int64_t &timestamp) = 0;
    virtual bool getLastReadTimestampInProducer(int64_t &timestamp) = 0;
    virtual bool producerSeek(const common::Locator &locator) = 0;
    bool getLatestLocator(common::Locator& locator, bool &fromInc);
    bool getLatestLocator(common::Locator& locator);
    void checkForceSeek();
    virtual bool seekProducerToLatest(std::pair<int64_t, int64_t>& forceSeekInfo) { return false; }
    
private:
    void checkRecoverBuild();
    void checkRecoverTime();
    void skipRtBeforeTimestamp();
    bool needAutoSuspend();
    bool needAutoResume();
    void autoSuspend();
    void autoResume();
    
protected:
    //virtual for test
    virtual BuildFlow *createBuildFlow(
        const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPartition,
        const util::SwiftClientCreatorPtr &swiftClientCreator) const;
private:
    static const int64_t CONTROL_INTERVAL = 500 * 1000; // 500000 us
    static const int64_t DEFAULT_RECOVER_LATENCY = 1000; // 1000 ms 

protected:
    AsyncStarter _starter;
    builder::Builder *_builder;
    std::unique_ptr<BuildFlow> _buildFlow;
    mutable autil::ThreadMutex _realtimeMutex;

    std::string _configPath;
    // keep index partition for retry init document reader
    IE_NAMESPACE(partition)::IndexPartitionPtr _indexPartition;
    IE_NAMESPACE(misc)::MetricProviderPtr _metricProvider;

    volatile bool _isRecovered;
    volatile bool _autoSuspend;
    volatile bool _adminSuspend;
    
private:
    int64_t _startRecoverTime;
    int64_t _maxRecoverTime;
    int64_t _recoverLatency;    
    int64_t _timestampToSkip;

    RealtimeErrorCode _errorCode;
    std::string _errorMsg;
    int64_t _errorTime;
    
    IE_NAMESPACE(util)::TaskSchedulerPtr _taskScheduler;
    int32_t _buildCtrlTaskId;
    
    bool _supportForceSeek;
    bool _seekToLatestWhenRecoverFail;  // build_option_config.seek_to_latest_when_recover_fail;
    bool _needForceSeek;
    std::pair<int64_t, int64_t> _forceSeekInfo; // (from, to)
    IE_NAMESPACE(misc)::MetricPtr _lostRt;

protected:
    proto::PartitionId _partitionId;
    util::SwiftClientCreatorPtr _swiftClientCreator;
    BuildFlowThreadResource _buildFlowThreadResource;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RealtimeBuilderImplBase);

}
}

#endif //ISEARCH_BS_REALTIMEBUILDERIMPLBASE_H
