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
#include <string>

#include "aios/autil/autil/Lock.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/worker/WorkerStateHandler.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/FlowFactory.h"

namespace build_service { namespace worker {

class ServiceWorker;

class BuildJobImpl : public WorkerStateHandler
{
public:
    BuildJobImpl(const proto::PartitionId& pid, indexlib::util::MetricProviderPtr metricProvider,
                 const proto::LongAddress& address, const std::string& appZkRoot = "",
                 const std::string& adminServiceName = "");
    ~BuildJobImpl();

private:
    BuildJobImpl(const BuildJobImpl&);
    BuildJobImpl& operator=(const BuildJobImpl&);

public:
    void doHandleTargetState(const std::string& state, bool hasResourceUpdated) override;
    bool needSuspendTask(const std::string& state) override;
    void getCurrentState(std::string& state) override;
    bool hasFatalError() override;

protected:
    virtual bool build(const proto::JobTarget& target);

private:
    virtual workflow::BuildFlow* createBuildFlow();

private:
    bool fillKeyValueMap(const config::ResourceReaderPtr& resourceReader, const proto::JobTarget& target,
                         KeyValueMap& kvMap);
    bool startBuildFlow(const config::ResourceReaderPtr& resourceReader, KeyValueMap& kvMap);

private:
    proto::JobCurrent _current;
    ServiceWorker* _worker;
    workflow::BuildFlow* _buildFlow;
    common::CounterSynchronizerPtr _counterSynchronizer;
    std::unique_ptr<workflow::FlowFactory> _brokerFactory;
    mutable autil::RecursiveThreadMutex _lock; // attention lock code should use less time
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildJobImpl);

}} // namespace build_service::worker
