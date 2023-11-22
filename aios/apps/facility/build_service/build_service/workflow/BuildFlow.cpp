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
#include "build_service/workflow/BuildFlow.h"

#include <algorithm>
#include <assert.h>
#include <functional>
#include <map>
#include <optional>
#include <ostream>
#include <stddef.h>
#include <string>
#include <typeinfo>
#include <unistd.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "beeper/beeper.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ControlConfig.h"
#include "build_service/config/DataLinkModeUtil.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/document/ClassifiedDocument.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/LocatorUtil.h"
#include "build_service/workflow/Consumer.h"
#include "build_service/workflow/DataFlowFactory.h"
#include "build_service/workflow/FlowFactory.h"
#include "build_service/workflow/Producer.h"
#include "indexlib/base/Progress.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/framework/Locator.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::proto;
using namespace build_service::document;
using namespace build_service::reader;
using namespace build_service::common;

using build_service::common::Locator;

namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, BuildFlow);

BuildFlow::BuildFlow(const indexlib::config::IndexPartitionSchemaPtr& schema,
                     const BuildFlowThreadResource& threadResource)
    : _schema(schema)
    , _factory(NULL)
    , _mode(BuildFlowMode::NONE)
    , _workflowMode(SERVICE)
    , _buildFlowFatalError(false)
    , _isSuspend(false)
    , _buildFlowThreadResource(threadResource)
    , _isTablet(false)
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
}

BuildFlow::BuildFlow(std::shared_ptr<indexlibv2::config::ITabletSchema> schema,
                     const BuildFlowThreadResource& threadResource)
    : _schema(nullptr)
    , _schemav2(schema)
    , _factory(NULL)
    , _mode(BuildFlowMode::NONE)
    , _workflowMode(SERVICE)
    , _buildFlowFatalError(false)
    , _isSuspend(false)
    , _buildFlowThreadResource(threadResource)
    , _isTablet(true)
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
}

BuildFlow::~BuildFlow()
{
    stop();
    for (auto& flow : _dataFlows) {
        flow.reset();
    }
    _factory = NULL;
}

void BuildFlow::startWorkLoop(const ResourceReaderPtr& resourceReader, const proto::PartitionId& partitionId,
                              KeyValueMap& kvMap, FlowFactory* brokerFactory, BuildFlowMode buildFlowMode,
                              WorkflowMode workflowMode, indexlib::util::MetricProviderPtr metricProvider)
{
    initBeeperTag(partitionId);
    _starter.setName("BsBuildFlow");
    _starter.asyncStart(bind(&BuildFlow::buildFlowWorkLoop, this, resourceReader, partitionId, kvMap, brokerFactory,
                             buildFlowMode, workflowMode, metricProvider));
}

bool BuildFlow::buildFlowWorkLoop(const ResourceReaderPtr& resourceReader, const proto::PartitionId& partitionId,
                                  KeyValueMap& kvMap, FlowFactory* brokerFactory, BuildFlowMode buildFlowMode,
                                  WorkflowMode workflowMode, indexlib::util::MetricProviderPtr metricProvider)
{
    if (!isStarted()) {
        BS_LOG(INFO, "[%s] start BuildFlow, buildFlowMode[%d], workflowMode[%d]",
               partitionId.buildid().ShortDebugString().c_str(), buildFlowMode, workflowMode);
        if (!startBuildFlow(resourceReader, partitionId, kvMap, brokerFactory, buildFlowMode, workflowMode,
                            metricProvider)) {
            BS_LOG(INFO, "[%s] BuildFlow failed, retry later", partitionId.buildid().ShortDebugString().c_str());
            return false;
        }
    }

    return true;
}

bool BuildFlow::initCounterMap(BuildFlowMode mode, FlowFactory* factory, FlowFactory::RoleInitParam& param)
{
    return factory->initCounterMap(param, mode);
}

void BuildFlow::fillInputOutputInfo(BuildFlowMode buildFlowMode, KeyValueMap& kvMap)
{
    if (kvMap.find(INPUT_DOC_TYPE) == kvMap.end()) {
        if (buildFlowMode & BuildFlowMode::PROCESSOR) {
            kvMap[INPUT_DOC_TYPE] = INPUT_DOC_RAW;
        } else if (buildFlowMode & BuildFlowMode::BUILDER) {
            kvMap[INPUT_DOC_TYPE] = INPUT_DOC_PROCESSED;
        } else {
            assert(false);
        }
    }
    if (kvMap.find(WORKER_HAS_OUTPUT) == kvMap.end()) {
        if (buildFlowMode == BuildFlowMode::ALL) {
            kvMap[WORKER_HAS_OUTPUT] = KV_FALSE;
        } else if (buildFlowMode == BuildFlowMode::PROCESSOR) {
            kvMap[WORKER_HAS_OUTPUT] = KV_TRUE;
        } else if (buildFlowMode == BuildFlowMode::BUILDER) {
            kvMap[WORKER_HAS_OUTPUT] = KV_FALSE;
        } else {
            assert(false);
        }
    }
}

bool BuildFlow::startBuildFlow(const ResourceReaderPtr& resourceReader, const proto::PartitionId& partitionId,
                               KeyValueMap& kvMap, FlowFactory* factory, BuildFlowMode buildFlowMode,
                               WorkflowMode workflowMode, indexlib::util::MetricProviderPtr metricProvider)
{
    autil::ScopedLock lock(_mutex);
    fillInputOutputInfo(buildFlowMode, kvMap);

    // clear members
    clear();
    _mode = buildFlowMode;
    _workflowMode = workflowMode;
    _factory = factory;
    _buildFlowFatalError = false;

    if (!initDataLinkParameters(resourceReader, partitionId, kvMap)) {
        BS_LOG(ERROR, "init DataLink parameters failed");
        return false;
    }
    // todo: determine workflow mode from BuildFlowMode: JOB or SERVICE
    FlowFactory::RoleInitParam param;
    if (!initRoleInitParam(resourceReader, partitionId, workflowMode, metricProvider, kvMap, param)) {
        return false;
    }
    _counterMap = param.counterMap;
    _dataFlows = DataFlowFactory::createDataFlow(param, _mode, _factory);
    if (_dataFlows.empty()) {
        BS_LOG(ERROR, "create data flow failed");
        clear();
        return false;
    }

    if (_isSuspend) {
        for (auto& flow : _dataFlows) {
            flow->suspend();
        }
    }

    if (!seek(_dataFlows, partitionId)) {
        clear();
        return false;
    }

    for (auto& flow : _dataFlows) {
        if (!startWorkflow(flow.get(), workflowMode, _buildFlowThreadResource.workflowThreadPool)) {
            clear();
            return false;
        }
    }
    return true;
}

bool BuildFlow::startWorkflow(Workflow* flow, WorkflowMode workflowMode,
                              const WorkflowThreadPoolPtr& workflowThreadPool)
{
    if (!flow) {
        return true;
    }
    if (!flow->start(workflowMode, workflowThreadPool)) {
        BS_LOG(ERROR, "start workflow[%s] failed.", typeid(*flow).name());

        string msg = "start workflow[" + string(typeid(*flow).name()) + "] failed.";
        BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, msg);
        return false;
    }
    return true;
}

bool BuildFlow::seek(std::vector<std::unique_ptr<Workflow>>& dataFlow, const proto::PartitionId& partitionId)
{
    if (dataFlow.empty()) {
        return true;
    }
    common::Locator minLocator;

    vector<common::Locator> locators;
    locators.reserve(dataFlow.size());
    for (auto iter = dataFlow.rbegin(); iter != dataFlow.rend(); ++iter) {
        auto& flow = *iter;
        common::Locator currentLocator;
        if (!flow->getConsumer()->getLocator(currentLocator)) {
            std::string errorMsg = std::string("consumer get locator failed");
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            REPORT_ERROR_WITH_ADVICE(proto::BUILDFLOW_ERROR_SEEK, errorMsg, proto::BS_RETRY);
            return false;
        }
        if (!currentLocator.IsValid()) {
            // do nothing
        } else {
            if (!minLocator.IsValid()) {
                minLocator = currentLocator;
            } else {
                if (!minLocator.IsSameSrc(currentLocator, true)) {
                    BS_LOG(ERROR, "not support diff src seek");
                    return false;
                }
                auto minProgressOptional = util::LocatorUtil::computeMultiProgress(
                    minLocator.GetMultiProgress(), currentLocator.GetMultiProgress(), util::LocatorUtil::minOffset);
                if (minProgressOptional.has_value()) {
                    minLocator.SetMultiProgress(minProgressOptional.value());
                } else {
                    BS_LOG(ERROR, "seek compute min offset failed");
                    return false;
                }
            }
        }
        locators.push_back(minLocator);
        BS_LOG(INFO, "locators push back locator [%s]", minLocator.DebugString().c_str());
    }
    reverse(locators.begin(), locators.end());
    for (size_t i = 0; i < dataFlow.size(); ++i) {
        ProducerBase* producer = dataFlow[i]->getProducer();
        auto& locator = locators[i];
        string msg = partitionId.buildid().ShortDebugString() + " flow " + StringUtil::toString(i) + " seek locator " +
                     locator.DebugString();
        BS_LOG(INFO, "%s", msg.c_str());
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
        if (locator.IsValid() && !producer->seek(locator)) {
            std::string errorMsg = std::string("producer seek locator: ") + locator.DebugString() + " failed.";
            REPORT_ERROR_WITH_ADVICE(proto::BUILDFLOW_ERROR_SEEK, errorMsg, proto::BS_RETRY);
            return false;
        }
    }
    return true;
}

void BuildFlow::stop(StopOption stopOption)
{
    _starter.stop();

    autil::ScopedLock lock(_mutex);
    for (auto& flow : _dataFlows) {
        flow->stop(stopOption);
    }
}

bool BuildFlow::waitFinish()
{
    while (!isFinished()) {
        if (hasFatalError()) {
            string errorMsg = "Wait finish failed, has fatal error.";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
            return false;
        }
        usleep(WAIT_FINISH_INTERVAL);
    }
    return true;
}

BuildFlowMode BuildFlow::getBuildFlowMode(proto::RoleType role)
{
    if (role == proto::ROLE_BUILDER) {
        return BuildFlowMode::BUILDER;
    } else if (role == proto::ROLE_PROCESSOR) {
        return BuildFlowMode::PROCESSOR;
    } else if (role == proto::ROLE_JOB) {
        return BuildFlowMode::ALL;
    }
    assert(false);
    return BuildFlowMode::ALL;
}

builder::Builder* BuildFlow::getBuilderUnsafe() const { return _factory ? _factory->getBuilder() : NULL; }
builder::BuilderV2* BuildFlow::getBuilderV2Unsafe() const { return _factory ? _factory->getBuilderV2() : nullptr; }

builder::Builder* BuildFlow::getBuilder() const
{
    autil::ScopedLock lock(_mutex);
    return getBuilderUnsafe();
}

builder::BuilderV2* BuildFlow::getBuilderV2() const
{
    autil::ScopedLock lock(_mutex);
    return getBuilderV2Unsafe();
}

processor::Processor* BuildFlow::getProcessorUnsafe() const { return _factory ? _factory->getProcessor() : NULL; }

processor::Processor* BuildFlow::getProcessor() const
{
    autil::ScopedLock lock(_mutex);
    return getProcessorUnsafe();
}

reader::RawDocumentReader* BuildFlow::getReaderUnsafe() const { return _factory ? _factory->getReader() : NULL; }

reader::RawDocumentReader* BuildFlow::getReader() const
{
    autil::ScopedLock lock(_mutex);
    return getReaderUnsafe();
}

bool BuildFlow::suspendReadAtTimestamp(int64_t inputStopTs, int64_t endBuildTs, common::ExceedTsAction action)
{
    autil::ScopedLock lock(_mutex);
    if (!getOutputFlowUnsafe() || !getOutputFlowUnsafe()->getConsumer()) {
        BS_LOG(ERROR,
               "output flow or output consumeris not exist, "
               "suspend buildFlow at %ld,",
               endBuildTs);
        return false;
    } else {
        getOutputFlowUnsafe()->getConsumer()->SetEndBuildTimestamp(endBuildTs);
        BS_LOG(INFO, "output flow consumer will set end build at timestamp[%ld]", endBuildTs);
        string msg = "output flow consumer will set end build at timestamp[" + StringUtil::toString(endBuildTs) + "]";
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    }
    if (!getInputFlowUnsafe() || !getInputFlowUnsafe()->getProducer()) {
        BS_LOG(ERROR,
               "input flow or input producer is not exist, "
               "suspend buildFlow at %ld, action[%d] failed",
               inputStopTs, action);
        return false;
    } else {
        getInputFlowUnsafe()->getProducer()->suspendReadAtTimestamp(inputStopTs, action);
        BS_LOG(INFO, "input producer will stop at timestamp[%ld]", inputStopTs);
        string msg = "input producer will  stop at timestamp[" + StringUtil::toString(inputStopTs) + "]";
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    }
    return true;
}

bool BuildFlow::isFinished() const
{
    autil::ScopedLock lock(_mutex);
    if (_dataFlows.size() == 0) {
        return false;
    }
    for (auto& flow : _dataFlows) {
        if (!flow->isFinished()) {
            return false;
        }
    }
    return true;
}

bool BuildFlow::isStarted() const
{
    autil::ScopedLock lock(_mutex);

    if (_mode == BuildFlowMode::NONE) {
        return false;
    }
    return !_dataFlows.empty();
}

void BuildFlow::resume()
{
    autil::ScopedLock lock(_mutex);
    _isSuspend = false;
    for (auto& flow : _dataFlows) {
        flow->resume();
    }
}

void BuildFlow::suspend()
{
    autil::ScopedLock lock(_mutex);
    _isSuspend = true;
    for (auto& flow : _dataFlows) {
        flow->suspend();
    }
}

void BuildFlow::collectSubModuleErrorInfos() const
{
    vector<proto::ErrorInfo> errorInfos;
    if (_factory) {
        _factory->collectErrorInfos(errorInfos);
    }

    reader::RawDocumentReader* reader = getReaderUnsafe();
    if (reader) {
        reader->fillErrorInfos(errorInfos);
    }

    processor::Processor* processor = getProcessorUnsafe();
    if (processor) {
        processor->fillErrorInfos(errorInfos);
    }

    builder::Builder* builder = getBuilderUnsafe();
    if (builder) {
        builder->fillErrorInfos(errorInfos);
    }

    addErrorInfos(errorInfos);
}

void BuildFlow::fillErrorInfos(deque<proto::ErrorInfo>& errorInfos) const
{
    collectSubModuleErrorInfos();
    ErrorCollector::fillErrorInfos(errorInfos);
}

bool BuildFlow::hasFatalError() const
{
    ScopedLock lock(_mutex);
    if (_buildFlowFatalError) {
        return true;
    }
    for (auto& flow : _dataFlows) {
        if (flow->hasFatalError()) {
            return true;
        }
    }
    return false;
}

bool BuildFlow::needReconstruct() const
{
    ScopedLock lock(_mutex);
    for (auto& flow : _dataFlows) {
        if (flow->needReconstruct()) {
            return true;
        }
    }
    return false;
}

bool BuildFlow::initDataLinkParameters(const ResourceReaderPtr& resourceReader, const PartitionId& partitionId,
                                       KeyValueMap& kvMap)
{
    string realTimeMode = getValueFromKeyValueMap(kvMap, REALTIME_MODE);
    if (realTimeMode == REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE) {
        kvMap[INPUT_DOC_TYPE] = INPUT_DOC_RAW;
    }
    if (config::DataLinkModeUtil::isDataLinkNPCMode(kvMap)) {
        kvMap[INPUT_DOC_TYPE] = INPUT_DOC_BATCHDOC;
    }
    bool hasBuilder = _mode & BuildFlowMode::BUILDER;
    if (hasBuilder && partitionId.step() == BUILD_STEP_INC) {
        config::ControlConfig controlConfig;
        if (resourceReader->getDataTableConfigWithJsonPath(partitionId.buildid().datatable(), "control_config",
                                                           controlConfig)) {
            if (partitionId.clusternames_size() == 0) {
                BS_LOG(ERROR, "bad partitionId[%s], no clustername defined", partitionId.ShortDebugString().c_str());
                return false;
            }
            const string& clusterName = partitionId.clusternames(0);
            if (controlConfig.getDataLinkMode() == ControlConfig::DataLinkMode::FP_INP_MODE &&
                !controlConfig.isIncProcessorExist(clusterName)) {
                kvMap[INPUT_DOC_TYPE] = INPUT_DOC_RAW;
                if (_mode & BuildFlowMode::PROCESSOR) {
                    assert(_mode == BuildFlowMode::ALL);
                    _mode = BuildFlowMode::BUILDER;
                }
            }
        }
    }
    return true;
}

bool BuildFlow::initRoleInitParam(const ResourceReaderPtr& resourceReader, const PartitionId& partitionId,
                                  WorkflowMode workflowMode, indexlib::util::MetricProviderPtr metricProvider,
                                  KeyValueMap& kvMap, FlowFactory::RoleInitParam& param)

{
    const string& clusterName = partitionId.clusternames(0);
    if (!initReaderParameters(resourceReader, clusterName, workflowMode, kvMap)) {
        return false;
    }
    param.resourceReader = resourceReader;
    param.kvMap = kvMap;
    param.partitionId = partitionId;
    param.metricProvider = metricProvider;
    param.schema = _schema;
    param.schemav2 = _schemav2;
    param.isTablet = _isTablet;
    if (!param.schema && !param.schemav2) {
        auto schema = resourceReader->getTabletSchema(clusterName);
        if (schema && schema->GetLegacySchema() && !schema->GetLegacySchema()->IsTablet()) {
            param.schema = schema->GetLegacySchema();
        } else {
            param.schemav2 = schema;
        }
    }
    if (!initCounterMap(_mode, _factory, param)) {
        BS_LOG(WARN, "init counter map failed");
    }
    return true;
}

bool BuildFlow::initReaderParameters(const ResourceReaderPtr& resourceReader, const std::string& clusterName,
                                     WorkflowMode workflowMode, KeyValueMap& kvMap)
{
    config::BuilderClusterConfig clusterConfig;
    string mergeConfigName = getValueFromKeyValueMap(kvMap, MERGE_CONFIG_NAME);
    config::BuilderConfig builderConfig;
    if (clusterConfig.init(clusterName, *resourceReader, mergeConfigName)) {
        builderConfig = clusterConfig.builderConfig;
    }
    string batchMask = getValueFromKeyValueMap(kvMap, BATCH_MASK);
    string useInnerBatchMaskFilter = getValueFromKeyValueMap(kvMap, USE_INNER_BATCH_MASK_FILTER);
    auto it = kvMap.find(DATA_DESCRIPTION_KEY);
    if (it != kvMap.end()) {
        SwiftConfig swiftConfig;
        if (!resourceReader->getConfigWithJsonPath(ResourceReader::getClusterConfRelativePath(clusterName),
                                                   "swift_config", swiftConfig)) {
            string errorMsg = "failed to parse swift_config for cluster: " + clusterName;
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        if (swiftConfig.hasTopicConfig(RAW_SWIFT_TOPIC_CONFIG)) {
            const auto& rawTopicConfig = swiftConfig.getTopicConfig(RAW_SWIFT_TOPIC_CONFIG);
            kvMap[RAW_TOPIC_SWIFT_READER_CONFIG] = rawTopicConfig->readerConfigStr;
        }
    }

    bool enableFastSlowQueue = clusterConfig.enableFastSlowQueue;
    if (!enableFastSlowQueue && kvMap.find(ENABLE_FAST_SLOW_QUEUE) != kvMap.end()) {
        enableFastSlowQueue |= (kvMap[ENABLE_FAST_SLOW_QUEUE] == "true");
    }
    if (enableFastSlowQueue) {
        // for middle-convert topic
        kvMap[ENABLE_FAST_SLOW_QUEUE] = StringUtil::toString((bool)enableFastSlowQueue);
        BS_LOG(INFO, "cluster: %s enable fast slow queue", clusterName.c_str());
    }
    kvMap[SWIFT_FILTER_MASK] = StringUtil::toString(0);
    kvMap[SWIFT_FILTER_RESULT] = StringUtil::toString(0);
    if (workflowMode == build_service::workflow::REALTIME) {
        if (enableFastSlowQueue) {
            if (builderConfig.documentFilter) {
                kvMap[SWIFT_FILTER_MASK] =
                    StringUtil::toString((uint8_t)(ProcessedDocument::SWIFT_FILTER_BIT_REALTIME |
                                                   ProcessedDocument::SWIFT_FILTER_BIT_FASTQUEUE));
                kvMap[FAST_QUEUE_SWIFT_FILTER_MASK] =
                    StringUtil::toString((uint8_t)(ProcessedDocument::SWIFT_FILTER_BIT_REALTIME |
                                                   ProcessedDocument::SWIFT_FILTER_BIT_FASTQUEUE));
            } else {
                kvMap[SWIFT_FILTER_MASK] = StringUtil::toString((uint8_t)ProcessedDocument::SWIFT_FILTER_BIT_FASTQUEUE);
                kvMap[FAST_QUEUE_SWIFT_FILTER_MASK] =
                    StringUtil::toString((uint8_t)ProcessedDocument::SWIFT_FILTER_BIT_FASTQUEUE);
            }
            kvMap[FAST_QUEUE_SWIFT_FILTER_RESULT] =
                StringUtil::toString((uint8_t)ProcessedDocument::SWIFT_FILTER_BIT_FASTQUEUE);
        } else {
            if (builderConfig.documentFilter) {
                kvMap[SWIFT_FILTER_MASK] = StringUtil::toString((uint8_t)ProcessedDocument::SWIFT_FILTER_BIT_REALTIME);
            } else {
                kvMap[SWIFT_FILTER_MASK] = StringUtil::toString((uint8_t)ProcessedDocument::SWIFT_FILTER_BIT_NONE);
            }
        }
    } else if (!batchMask.empty() && batchMask != "") {
        // for batch mode
        int32_t batchMaskValue = 0;
        if (!StringUtil::fromString(batchMask, batchMaskValue)) {
            BS_LOG(ERROR, "invalid batch mask[%s]", batchMask.c_str());
            return false;
        }
        if (batchMaskValue < 0 || batchMaskValue > ProcessedDocument::SWIFT_FILTER_BIT_ALL) {
            kvMap[SWIFT_FILTER_MASK] = StringUtil::toString((uint8_t)ProcessedDocument::SWIFT_FILTER_BIT_NONE);
        } else {
            kvMap[SWIFT_FILTER_MASK] = StringUtil::toString((uint8_t)ProcessedDocument::SWIFT_FILTER_BIT_ALL);
            kvMap[SWIFT_FILTER_RESULT] = StringUtil::toString((uint8_t)batchMaskValue);
            kvMap[IS_BATCH_MODE] = KV_TRUE;
            stringstream ss;
            ss << "uint8FilterMask=" << kvMap[SWIFT_FILTER_MASK].c_str() << ";"
               << "uint8MaskResult=" << kvMap[SWIFT_FILTER_RESULT].c_str();
            kvMap[RAW_TOPIC_SWIFT_READER_CONFIG] = ss.str();
            BS_LOG(INFO, "buildFlow enable BatchMask [%d]", batchMaskValue);
        }
    } else {
        kvMap[SWIFT_FILTER_MASK] = StringUtil::toString((uint8_t)ProcessedDocument::SWIFT_FILTER_BIT_NONE);
        if (enableFastSlowQueue) {
            kvMap[SWIFT_FILTER_MASK] = StringUtil::toString((uint8_t)ProcessedDocument::SWIFT_FILTER_BIT_FASTQUEUE);
            kvMap[FAST_QUEUE_SWIFT_FILTER_MASK] =
                StringUtil::toString((uint8_t)ProcessedDocument::SWIFT_FILTER_BIT_FASTQUEUE);
            kvMap[FAST_QUEUE_SWIFT_FILTER_RESULT] =
                StringUtil::toString((uint8_t)ProcessedDocument::SWIFT_FILTER_BIT_FASTQUEUE);
        }
    }

    it = kvMap.find(RAW_TOPIC_SWIFT_READER_CONFIG);
    if (it == kvMap.end()) {
        BS_LOG(INFO, "init swift filter mask[%s], swift filter result[%s], use bs inner mask filter [%s].",
               kvMap[SWIFT_FILTER_MASK].c_str(), kvMap[SWIFT_FILTER_RESULT].c_str(), useInnerBatchMaskFilter.c_str());
    } else {
        BS_LOG(INFO, "init swift topic reader config = [%s], use bs inner mask filter [%s]", it->second.c_str(),
               useInnerBatchMaskFilter.c_str());
    }
    return true;
}

void BuildFlow::clear()
{
    collectSubModuleErrorInfos();
    _dataFlows.clear();
    BS_LOG(INFO, "clear build flow");
}

bool BuildFlow::getLocator(Locator& locator) const
{
    if (_dataFlows.empty()) {
        return false;
    }
    return getOutputFlowUnsafe()->getConsumer()->getLocator(locator);
}

}} // namespace build_service::workflow
