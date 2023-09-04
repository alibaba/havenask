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

#include "build_service/util/Log.h"
#include "build_service/workflow/RealtimeBuilderImplV2.h"
#include "build_service/workflow/Workflow.h"

namespace build_service { namespace workflow {

class FlowFactory;
class SwiftProcessedDocProducer;

class ProcessedDocRtBuilderImplV2 : public RealtimeBuilderImplV2
{
public:
    ProcessedDocRtBuilderImplV2(const std::string& configPath, std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                                const RealtimeBuilderResource& builderResource,
                                future_lite::NamedTaskScheduler* tasker);

    virtual ~ProcessedDocRtBuilderImplV2();

private:
    ProcessedDocRtBuilderImplV2(const ProcessedDocRtBuilderImplV2&);
    ProcessedDocRtBuilderImplV2& operator=(const ProcessedDocRtBuilderImplV2&);

private:
    class ForceSeekStatus;

protected: /*override*/
    bool doStart(const proto::PartitionId& partitionId, KeyValueMap& kvMap) override;
    bool getLastTimestampInProducer(int64_t& timestamp) override;
    bool getLastReadTimestampInProducer(int64_t& timestamp) override;
    bool producerAndBuilderPrepared() const override;
    bool producerSeek(const common::Locator& locator) override;
    void handleRecoverTimeout() override;
    void externalActions() override;

private:
    virtual common::ResourceKeeperMap createResources(const config::ResourceReaderPtr& resourceReader,
                                                      const proto::PartitionId& pid, KeyValueMap& kvMap) const;

    bool prepareForNormalMode(const config::ResourceReaderPtr& resourceReader, const proto::PartitionId& partitionId,
                              KeyValueMap& kvMap);

    bool prepareForNPCMode(const config::ResourceReaderPtr& resourceReader, const proto::PartitionId& partitionId,
                           KeyValueMap& kvMap);

    bool getBuilderAndProducer();
    void reportFreshnessWhenSuspendBuild();
    // void resetStartSkipTimestamp(KeyValueMap& kvMap);
    bool seekProducerToLatest() override;
    void setProducerRecovered();
    void reportProducerSkipInterval();

protected:
    // virtual for test
    virtual FlowFactory* createFlowFactory(const config::ResourceReaderPtr& resourceReader,
                                           const proto::PartitionId& pid, KeyValueMap& kvMap) const;
    // virtual int64_t getStartSkipTimestamp() const;

private:
    std::unique_ptr<FlowFactory> _brokerFactory;
    SwiftProcessedDocProducer* _producer;
    bool _startSkipCalibrateDone; // for flushed realtime index, need skip to rt locator
    bool _seekToLatestInForceRecover;
    common::Locator _lastSkipedLocator;
    bool _isProducerRecovered {false};
    indexlib::util::MetricPtr _lostRtMetric;
    std::unique_ptr<ForceSeekStatus> _forceSeekStatus;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::workflow
