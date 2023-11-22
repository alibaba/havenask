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
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <stdint.h>
#include <string>
#include <utility>

#include "build_service/builder/BuilderV2.h"
#include "build_service/common/Locator.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/workflow/AsyncStarter.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "build_service/workflow/RealtimeErrorDefine.h"
#include "build_service/workflow/StopOption.h"
#include "future_lite/TaskScheduler.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/VersionMeta.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace util {
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
}} // namespace indexlib::util

namespace future_lite {
class NamedTaskScheduler;
}

namespace build_service { namespace workflow {

class RealtimeBuilderImplV2
{
public:
    RealtimeBuilderImplV2(const std::string& configPath, std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                          const RealtimeBuilderResource& builderResource, future_lite::NamedTaskScheduler* tasker);

    virtual ~RealtimeBuilderImplV2();

    RealtimeBuilderImplV2(const RealtimeBuilderImplV2&) = delete;
    RealtimeBuilderImplV2& operator=(const RealtimeBuilderImplV2&) = delete;

public:
    bool start(const proto::PartitionId& partitionId, KeyValueMap& kvMap);
    void stop(StopOption stopOption);
    void suspendBuild();
    void resumeBuild();
    void forceRecover();
    void setTimestampToSkip(int64_t timestamp);
    bool isRecovered();
    bool needReconstruct();
    bool hasFatalError();
    bool isFinished();
    void getErrorInfo(RealtimeErrorCode& errorCode, std::string& errorMsg, int64_t& errorTime) const;
    void executeBuildControlTask();
    bool needCommit() const;
    std::pair<indexlib::Status, indexlibv2::framework::VersionMeta> commit();

    const proto::PartitionId& getPartitionId() const { return _partitionId; }
    virtual bool needAlterTable() const = 0;
    virtual schemaid_t getAlterTableSchemaId() const = 0;

protected:
    virtual bool doStart(const proto::PartitionId& partitionId, KeyValueMap& kvMap) = 0;
    virtual void handleRecoverTimeout() {}

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
    virtual bool seekProducerToLatest() { return false; }
    indexlibv2::framework::Locator getLatestLocator() const;
    void checkForceSeek();

private:
    void checkRecoverBuild();
    void checkRecoverTime();
    void skipRtBeforeTimestamp();
    bool needAutoSuspend();
    bool needAutoResume();
    void autoSuspend();
    void autoResume();
    void stopTask();
    void setIsRecovered(bool isRecovered);

protected:
    // virtual for test
    virtual BuildFlow* createBuildFlow(indexlibv2::framework::ITablet* tablet,
                                       const util::SwiftClientCreatorPtr& swiftClientCreator) const;

private:
    static constexpr std::chrono::duration CONTROL_INTERVAL = std::chrono::milliseconds(500);
    static constexpr int64_t DEFAULT_RECOVER_LATENCY = 1000; // 1000 ms

protected:
    AsyncStarter _starter;
    builder::BuilderV2* _builder;
    std::unique_ptr<BuildFlow> _buildFlow;
    mutable std::mutex _realtimeMutex;

    std::string _configPath;
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;

    indexlib::util::MetricProviderPtr _metricProvider;

    std::atomic<bool> _isRecovered;
    std::atomic<bool> _autoSuspend;
    std::atomic<bool> _adminSuspend;

private:
    int64_t _startRecoverTime;
    int64_t _maxRecoverTime;
    int64_t _recoverLatency;
    int64_t _timestampToSkip;

    RealtimeErrorCode _errorCode;
    std::string _errorMsg;
    int64_t _errorTime;

    int32_t _buildCtrlTaskId;
    future_lite::TaskScheduler::Handle _buildCtrlTaskHandle;
    std::function<void()> _reconstructor;

protected:
    proto::PartitionId _partitionId;
    util::SwiftClientCreatorPtr _swiftClientCreator;
    BuildFlowThreadResource _buildFlowThreadResource;
    future_lite::NamedTaskScheduler* _tasker;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RealtimeBuilderImplV2);

}} // namespace build_service::workflow
