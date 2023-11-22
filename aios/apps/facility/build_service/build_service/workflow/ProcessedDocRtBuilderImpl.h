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

#include <memory>
#include <stdint.h>
#include <string>

#include "build_service/common/Locator.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/ResourceKeeper.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/workflow/FlowFactory.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "build_service/workflow/RealtimeBuilderImplBase.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "indexlib/partition/index_partition.h"

namespace build_service { namespace workflow {

class ProcessedDocRtBuilderImpl : public RealtimeBuilderImplBase
{
public:
    ProcessedDocRtBuilderImpl(const std::string& configPath, const indexlib::partition::IndexPartitionPtr& indexPart,
                              const RealtimeBuilderResource& builderResource);
    virtual ~ProcessedDocRtBuilderImpl();

private:
    ProcessedDocRtBuilderImpl(const ProcessedDocRtBuilderImpl&);
    ProcessedDocRtBuilderImpl& operator=(const ProcessedDocRtBuilderImpl&);

protected: /*override*/
    bool doStart(const proto::PartitionId& partitionId, KeyValueMap& kvMap) override;
    bool getLastTimestampInProducer(int64_t& timestamp) override;
    bool getLastReadTimestampInProducer(int64_t& timestamp) override;
    bool producerAndBuilderPrepared() const override;
    bool producerSeek(const common::Locator& locator) override;
    void externalActions() override;

private:
    virtual common::ResourceKeeperMap createResources(const config::ResourceReaderPtr& resourceReader,
                                                      const proto::PartitionId& pid, KeyValueMap& kvMap) const;
    bool getBuilderAndProducer();
    void skipToLatestLocator();
    void reportFreshnessWhenSuspendBuild();
    void resetStartSkipTimestamp(KeyValueMap& kvMap);
    void setProducerRecovered();

protected:
    // virtual for test
    virtual FlowFactory* createFlowFactory(const config::ResourceReaderPtr& resourceReader,
                                           const proto::PartitionId& pid, KeyValueMap& kvMap) const;
    virtual int64_t getStartSkipTimestamp() const;

private:
    std::unique_ptr<FlowFactory> _brokerFactory;
    SwiftProcessedDocProducer* _producer;
    bool _startSkipCalibrateDone; // for flushed realtime index, need skip to rt locator
    common::Locator _lastSkipedLocator;
    bool _isProducerRecovered {false};

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::workflow
