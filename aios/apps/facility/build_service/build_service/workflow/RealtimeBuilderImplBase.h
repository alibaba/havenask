/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "autil/Lock.h"
#include "build_service/builder/Builder.h"
#include "build_service/common/Locator.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/workflow/AsyncStarter.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "build_service/workflow/RealtimeErrorDefine.h"
#include "build_service/workflow/SwiftProcessedDocConsumer.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace util {
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
}} // namespace indexlib::util

namespace build_service { namespace workflow {

class RealtimeBuilderImplBase
{
public:
    RealtimeBuilderImplBase(const std::string& configPath, const indexlib::partition::IndexPartitionPtr& indexPart,
                            const RealtimeBuilderResource& builderResource, bool supportForceSeek);

    virtual ~RealtimeBuilderImplBase();

private:
    RealtimeBuilderImplBase(const RealtimeBuilderImplBase&);
    RealtimeBuilderImplBase& operator=(const RealtimeBuilderImplBase&);

public:
    bool start(const proto::PartitionId& partitionId, KeyValueMap& kvMap);
    void stop();
    void suspendBuild();
    void resumeBuild();
    void forceRecover();
    void setTimestampToSkip(int64_t timestamp);
    bool isRecovered();
    void getErrorInfo(RealtimeErrorCode& errorCode, std::string& errorMsg, int64_t& errorTime) const;
    void executeBuildControlTask();
    builder::Builder* GetBuilder() const { return _builder; }

protected:
    virtual bool doStart(const proto::PartitionId& partitionId, KeyValueMap& kvMap) = 0;

protected:
    bool prepareIntervalTask(const config::ResourceReaderPtr& resourceReader, const proto::PartitionId& partitionId,
                             indexlib::util::MetricProviderPtr metricProvider);

    void setErrorInfoUnsafe(RealtimeErrorCode errorCode, const std::string& errorMsg);

protected:
    virtual bool producerAndBuilderPrepared() const = 0;
    virtual void externalActions() = 0;
    virtual bool getLastTimestampInProducer(int64_t& timestamp) = 0;
    virtual bool getLastReadTimestampInProducer(int64_t& timestamp) = 0;
    virtual bool producerSeek(const common::Locator& locator) = 0;
    bool getLatestLocator(common::Locator& locator, bool& fromInc);
    bool getLatestLocator(common::Locator& locator);
    void checkForceSeek();
    virtual bool seekProducerToLatest(std::pair<int64_t, int64_t>& forceSeekInfo) { return false; }

private:
    void setIsRecovered(bool isRecovered);
    void checkRecoverBuild();
    void checkRecoverTime();
    void skipRtBeforeTimestamp();
    bool needAutoSuspend();
    bool needAutoResume();
    void autoSuspend();
    void autoResume();

protected:
    // virtual for test
    virtual BuildFlow* createBuildFlow(const indexlib::partition::IndexPartitionPtr& indexPartition,
                                       const util::SwiftClientCreatorPtr& swiftClientCreator) const;

private:
    static const int64_t CONTROL_INTERVAL = 500 * 1000;  // 500000 us
    static const int64_t DEFAULT_RECOVER_LATENCY = 1000; // 1000 ms

protected:
    AsyncStarter _starter;
    builder::Builder* _builder;
    std::unique_ptr<BuildFlow> _buildFlow;
    mutable autil::ThreadMutex _realtimeMutex;

    std::string _configPath;
    // keep index partition for retry init document reader
    indexlib::partition::IndexPartitionPtr _indexPartition;
    indexlib::util::MetricProviderPtr _metricProvider;

    std::atomic<bool> _isRecovered;
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

    int32_t _buildCtrlTaskId;

    bool _supportForceSeek;
    bool _seekToLatestWhenRecoverFail; // build_option_config.seek_to_latest_when_recover_fail;
    bool _needForceSeek;
    std::pair<int64_t, int64_t> _forceSeekInfo; // (from, to)
    indexlib::util::MetricPtr _lostRt;

protected:
    proto::PartitionId _partitionId;
    util::SwiftClientCreatorPtr _swiftClientCreator;
    BuildFlowThreadResource _buildFlowThreadResource;
    indexlib::util::TaskSchedulerPtr _taskScheduler;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RealtimeBuilderImplBase);

}} // namespace build_service::workflow
