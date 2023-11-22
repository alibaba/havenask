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

#include <deque>
#include <memory>
#include <stdint.h>
#include <vector>

#include "autil/Lock.h"
#include "build_service/builder/Builder.h"
#include "build_service/builder/BuilderV2.h"
#include "build_service/common/ExceedTsAction.h"
#include "build_service/common/Locator.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/SourceEnd2EndLatencyReporter.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/processor/Processor.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/workflow/AsyncStarter.h"
#include "build_service/workflow/BuildFlowMode.h"
#include "build_service/workflow/FlowFactory.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "build_service/workflow/StopOption.h"
#include "build_service/workflow/SwiftProcessedDocConsumer.h"
#include "build_service/workflow/Workflow.h"
#include "build_service/workflow/WorkflowItem.h"
#include "build_service/workflow/WorkflowThreadPool.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace util {
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
}} // namespace indexlib::util

namespace build_service { namespace workflow {

class BuildFlow : public proto::ErrorCollector
{
public:
    BuildFlow(const indexlib::config::IndexPartitionSchemaPtr& schema = nullptr,
              const BuildFlowThreadResource& threadResource = BuildFlowThreadResource());

    BuildFlow(std::shared_ptr<indexlibv2::config::ITabletSchema> schema, const BuildFlowThreadResource& threadResource);

    virtual ~BuildFlow();

private:
    BuildFlow(const BuildFlow&);
    BuildFlow& operator=(const BuildFlow&);

public:
    // REALTIME
    void startWorkLoop(const config::ResourceReaderPtr& resourceReader, const proto::PartitionId& partitionId,
                       KeyValueMap& kvMap, FlowFactory* factory, BuildFlowMode buildFlowMode, WorkflowMode workflowMode,
                       indexlib::util::MetricProviderPtr metricProvider);

    // SERVICE or JOB
    // virtual for test
    virtual bool startBuildFlow(const config::ResourceReaderPtr& resourceReader, const proto::PartitionId& partitionId,
                                KeyValueMap& kvMap, FlowFactory* factory, BuildFlowMode buildFlowMode,
                                WorkflowMode workflowMode, indexlib::util::MetricProviderPtr metricProvider);
    bool waitFinish();
    virtual void stop(StopOption stopOption = StopOption::SO_NORMAL);
    bool hasFatalError() const;
    bool needReconstruct() const;
    void resume();
    void suspend();
    virtual void fillErrorInfos(std::deque<proto::ErrorInfo>& errorInfos) const;

public:
    static BuildFlowMode getBuildFlowMode(proto::RoleType role);

public:
    Workflow* getInputFlow()
    {
        autil::ScopedLock lock(_mutex);
        return getInputFlowUnsafe();
    }
    Workflow* getOutputFlow()
    {
        autil::ScopedLock lock(_mutex);
        return getOutputFlowUnsafe();
    }

    virtual bool isStarted() const;
    virtual bool isFinished() const;
    virtual builder::Builder* getBuilder() const;
    virtual builder::BuilderV2* getBuilderV2() const;
    virtual processor::Processor* getProcessor() const;
    virtual reader::RawDocumentReader* getReader() const;

    virtual bool suspendReadAtTimestamp(int64_t inputStopTs, int64_t endBuildTs, common::ExceedTsAction action);

    virtual bool getLocator(common::Locator& locator) const;
    indexlib::util::CounterMapPtr getCounterMap() const { return _counterMap; }

private:
    bool buildFlowWorkLoop(const config::ResourceReaderPtr& resourceReader, const proto::PartitionId& partitionId,
                           KeyValueMap& kvMap, FlowFactory* brokerFactory, BuildFlowMode buildFlowMode,
                           WorkflowMode workflowMode, indexlib::util::MetricProviderPtr metricProvider);
    bool initWorkflows(FlowFactory* brokerFactory, const KeyValueMap& kvMap);

    virtual bool initRoleInitParam(const config::ResourceReaderPtr& resourceReader,
                                   const proto::PartitionId& partitionId, WorkflowMode workflowMode,
                                   indexlib::util::MetricProviderPtr metricProvider, KeyValueMap& kvMap,
                                   FlowFactory::RoleInitParam& param);
    bool initDataLinkParameters(const config::ResourceReaderPtr& resourceReader, const proto::PartitionId& partitionId,
                                KeyValueMap& kvMap);

    bool initReaderParameters(const config::ResourceReaderPtr& resourceReader, const std::string& clusterName,
                              WorkflowMode workflowMode, KeyValueMap& kvMap);

    bool initCounterMap(BuildFlowMode mode, FlowFactory* factory, FlowFactory::RoleInitParam& param);

    bool startWorkflow(Workflow* flow, WorkflowMode workflowMode, const WorkflowThreadPoolPtr& workflowThreadPool);

    bool seek(std::vector<std::unique_ptr<Workflow>>& dataFlow, const proto::PartitionId& partitionId);
    void fillInputOutputInfo(BuildFlowMode buildFlowMode, KeyValueMap& kvMap);

    builder::Builder* getBuilderUnsafe() const;
    builder::BuilderV2* getBuilderV2Unsafe() const;
    processor::Processor* getProcessorUnsafe() const;
    reader::RawDocumentReader* getReaderUnsafe() const;

    void collectSubModuleErrorInfos() const;
    void clear();

    Workflow* getInputFlowUnsafe() const { return _dataFlows.empty() ? nullptr : (*_dataFlows.begin()).get(); }
    Workflow* getOutputFlowUnsafe() const { return _dataFlows.empty() ? nullptr : (*_dataFlows.rbegin()).get(); }

private:
    static const uint32_t WAIT_FINISH_INTERVAL = 1000 * 1000; // 1s
private:
    AsyncStarter _starter;
    indexlib::config::IndexPartitionSchemaPtr _schema;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schemav2;
    std::vector<std::unique_ptr<Workflow>> _dataFlows;
    FlowFactory* _factory;
    BuildFlowMode _mode;
    volatile WorkflowMode _workflowMode;
    volatile bool _buildFlowFatalError;
    volatile bool _isSuspend;
    mutable autil::ThreadMutex _mutex;
    indexlib::util::CounterMapPtr _counterMap;
    BuildFlowThreadResource _buildFlowThreadResource;
    bool _isTablet = false;

private:
    friend class BuildFlowTest;
    BS_LOG_DECLARE();
};

}} // namespace build_service::workflow
