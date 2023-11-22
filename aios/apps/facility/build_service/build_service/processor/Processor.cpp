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
#include "build_service/processor/Processor.h"

#include <algorithm>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <map>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/Locator.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ProcessorChainSelectorConfig.h"
#include "build_service/config/ProcessorConfig.h"
#include "build_service/config/ProcessorInfo.h"
#include "build_service/document/ClassifiedDocument.h"
#include "build_service/processor/DocumentProcessorChainCreator.h"
#include "build_service/processor/DocumentProcessorChainCreatorV2.h"
#include "build_service/processor/MultiThreadProcessorWorkItemExecutor.h"
#include "build_service/processor/ProcessorChainSelector.h"
#include "build_service/processor/ProcessorWorkItem.h"
#include "build_service/processor/RegionDocumentProcessor.h"
#include "build_service/processor/SingleThreadProcessorWorkItemExecutor.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/util/Monitor.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/module_info.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/counter/CounterMap.h"
#include "kmonitor/client/MetricType.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::document;
using namespace build_service::common;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::config;

namespace build_service { namespace processor {

BS_LOG_SETUP(processor, Processor);

Processor::Processor(const string& strategyParam)
    : _strategyParam(strategyParam)
    , _stopped(false)
    , _dropped(false)
    , _sealed(false)
    , _enableRewriteDeleteSubDoc(false)
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
}

Processor::~Processor() { stop(/*instant*/ false); }

bool Processor::start(const config::ProcessorConfigReaderPtr& resourceReaderPtr, const proto::PartitionId& partitionId,
                      indexlib::util::MetricProviderPtr metricProvider, const CounterMapPtr& counterMap,
                      const KeyValueMap& kvMap, bool forceSingleThreaded, bool isTablet)
{
    initBeeperTag(partitionId);
    _metricProvider = metricProvider;
    _processorConfigReader = resourceReaderPtr;

    if (!counterMap) {
        BS_LOG(WARN, "counter map is NULL");
    }

    _processErrorCounter = GET_ACC_COUNTER(counterMap, processError);
    _processDocCountCounter = GET_ACC_COUNTER(counterMap, processDocCount);
    if (!_processErrorCounter) {
        BS_LOG(WARN, "create processErrorCounter failed");
    }
    if (!_processDocCountCounter) {
        BS_LOG(WARN, "create processDocCountCounter failed");
    }

    if (!loadChain(resourceReaderPtr, partitionId, counterMap, kvMap, isTablet)) {
        return false;
    }
    ProcessorConfig processorConfig;
    if (!resourceReaderPtr->getProcessorConfig("processor_config", &processorConfig)) {
        string errorMsg = "Get processor_config from [" + partitionId.buildid().datatable() + "]failed.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }
    _enableRewriteDeleteSubDoc = processorConfig.enableRewriteDeleteSubDoc;

    _waitProcessCountMetric = DECLARE_METRIC(_metricProvider, "perf/waitProcessCount", kmonitor::GAUGE, "count");
    if (!_reporter.declareMetrics(_metricProvider)) {
        string errorMsg = "processor init metrics failed";
        REPORT_ERROR_WITH_ADVICE(PROCESSOR_ERROR_INIT_METRICS, errorMsg, BS_RETRY);
        return false;
    }

    if (!forceSingleThreaded && processorConfig.processorThreadNum > 1) {
        _executor.reset(new MultiThreadProcessorWorkItemExecutor(processorConfig.processorThreadNum,
                                                                 processorConfig.processorQueueSize));
    } else {
        _executor.reset(new SingleThreadProcessorWorkItemExecutor(processorConfig.processorQueueSize));
    }
    return _executor->start();
}

bool Processor::initProcessorChainSelector(const config::ProcessorConfigReaderPtr& resourceReader,
                                           const proto::PartitionId& partitionId,
                                           const DocProcessorChainConfigVecPtr& chainConfig)
{
    ProcessorChainSelectorConfigPtr selectorConfig(new ProcessorChainSelectorConfig);
    bool isExist = false;
    if (!resourceReader->getProcessorConfig("processor_chain_selector_config", selectorConfig.get(), &isExist)) {
        BS_LOG(ERROR, "get processor_chain_selector_config failed");
    }

    if (!isExist) {
        BS_LOG(INFO, "init defalut processor_chain_selector_config");
        selectorConfig.reset();
    }
    _chainSelector.reset(new ProcessorChainSelector);
    if (!_chainSelector->init(chainConfig, selectorConfig)) {
        string errorMsg = "init ProcessorChainSelector failed.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }
    return true;
}

bool Processor::loadChain(const config::ProcessorConfigReaderPtr& resourceReader, const proto::PartitionId& partitionId,
                          const CounterMapPtr& counterMap, const KeyValueMap& kvMap, bool isTablet)
{
    if (isTablet) {
        return loadChainV2(resourceReader, partitionId, counterMap, kvMap);
    } else {
        return loadChain(resourceReader, partitionId, counterMap, kvMap);
    }
}

bool Processor::loadChain(const config::ProcessorConfigReaderPtr& resourceReader, const proto::PartitionId& partitionId,
                          const CounterMapPtr& counterMap, const KeyValueMap& kvMap)
{
    DocumentProcessorChainCreator creator;
    if (!creator.init(resourceReader->getResourceReader(), _metricProvider, counterMap)) {
        string errorMsg = "DocumentProcessorChainCreator init failed.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }
    DocProcessorChainConfigVec chainConfig;
    if (!resourceReader->getProcessorConfig("processor_chain_config", &chainConfig)) {
        string errorMsg = "Get processor_chain_config for [" + partitionId.buildid().datatable() + "] failed.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }

    vector<string> pidClusterNames = ProtoUtil::getClusterNames(partitionId);
    DocProcessorChainConfigVec newChainConfig = rewriteRegionDocumentProcessorConfig(resourceReader, chainConfig);

    for (size_t i = 0; i < newChainConfig.size(); ++i) {
        if (!newChainConfig[i].validate()) {
            string errorMsg = "Validate processor_chain_config for [" + partitionId.buildid().datatable() + "] failed.";
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return false;
        }
    }

    DocumentProcessorChainVecPtr chains(new DocumentProcessorChainVec);
    DocProcessorChainConfigVecPtr finalChainConfig(new DocProcessorChainConfigVec);
    for (size_t i = 0; i < newChainConfig.size(); ++i) {
        vector<string> clusterNames = getClusterNames(newChainConfig[i], pidClusterNames);
        if (clusterNames.empty()) {
            continue;
        }
        DocumentProcessorChainPtr chain = creator.create(newChainConfig[i], clusterNames);
        if (!chain) {
            string errorMsg =
                string("Create processor chain for table: ") + ToJsonString(newChainConfig[i]) + " failed.";
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return false;
        }

        if (!chain->initQueryEvaluator(resourceReader->getResourceReader(), kvMap)) {
            string errorMsg = string("Create processor chain for table: ") + ToJsonString(newChainConfig[i]) +
                              " failed: init query evaluater fail.";
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return false;
        }
        chains->push_back(chain);
        finalChainConfig->push_back(newChainConfig[i]);
        BS_LOG(INFO, "create chain for [%s]", StringUtil::toString(clusterNames, " ").c_str());
    }
    if (chains->size() == 0) {
        string errorMsg = "no processor chain to process!";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }
    // make sure chainConfig is according to the chains
    BS_LOG(INFO, "final chain size[%lu]", finalChainConfig->size());
    if (!initProcessorChainSelector(resourceReader, partitionId, finalChainConfig)) {
        string errorMsg = "processor init chain selector failed";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }

    setProcessorChains(chains);
    return true;
}

bool Processor::loadChainV2(const config::ProcessorConfigReaderPtr& resourceReader,
                            const proto::PartitionId& partitionId, const CounterMapPtr& counterMap,
                            const KeyValueMap& kvMap)
{
    DocumentProcessorChainCreatorV2 creator;
    if (!creator.init(resourceReader->getResourceReader(), _metricProvider, counterMap)) {
        string errorMsg = "DocumentProcessorChainCreator init failed.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }
    DocProcessorChainConfigVec chainConfig;
    if (!resourceReader->getProcessorConfig("processor_chain_config", &chainConfig)) {
        string errorMsg = "Get processor_chain_config for [" + partitionId.buildid().datatable() + "] failed.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }

    vector<string> pidClusterNames = ProtoUtil::getClusterNames(partitionId);
    DocProcessorChainConfigVec newChainConfig = rewriteRegionDocumentProcessorConfig(resourceReader, chainConfig);

    for (size_t i = 0; i < newChainConfig.size(); ++i) {
        if (!newChainConfig[i].validate()) {
            string errorMsg = "Validate processor_chain_config for [" + partitionId.buildid().datatable() + "] failed.";
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return false;
        }
    }

    DocumentProcessorChainVecPtr chains(new DocumentProcessorChainVec);
    DocProcessorChainConfigVecPtr finalChainConfig(new DocProcessorChainConfigVec);
    for (size_t i = 0; i < newChainConfig.size(); ++i) {
        vector<string> clusterNames = getClusterNames(newChainConfig[i], pidClusterNames);
        if (clusterNames.empty()) {
            continue;
        }
        DocumentProcessorChainPtr chain = creator.create(newChainConfig[i], clusterNames, kvMap);
        if (!chain) {
            string errorMsg =
                string("Create processor chain for table: ") + ToJsonString(newChainConfig[i]) + " failed.";
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return false;
        }

        if (!chain->initQueryEvaluator(resourceReader->getResourceReader(), kvMap)) {
            string errorMsg = string("Create processor chain for table: ") + ToJsonString(newChainConfig[i]) +
                              " failed: init query evaluater fail.";
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return false;
        }
        chains->push_back(chain);
        finalChainConfig->push_back(newChainConfig[i]);
        BS_LOG(INFO, "create chain for [%s]", StringUtil::toString(clusterNames, " ").c_str());
    }
    if (chains->size() == 0) {
        string errorMsg = "no processor chain to process!";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }
    // make sure chainConfig is according to the chains
    BS_LOG(INFO, "final chain size[%lu]", finalChainConfig->size());
    if (!initProcessorChainSelector(resourceReader, partitionId, finalChainConfig)) {
        string errorMsg = "processor init chain selector failed";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }

    setProcessorChains(chains);
    return true;
}

DocProcessorChainConfigVec
Processor::rewriteRegionDocumentProcessorConfig(const ProcessorConfigReaderPtr& resourceReader,
                                                const DocProcessorChainConfigVec& inChains) const
{
    DocProcessorChainConfigVec newChains;
    DocProcessorChainConfigVec emptyChains;
    for (size_t i = 0; i < inChains.size(); ++i) {
        bool needRewrite = false;
        size_t regionProcessorIdx = 0;
        const ProcessorInfos& processorInfos = inChains[i].processorInfos;

        for (size_t i = 0; i < processorInfos.size(); ++i) {
            if (processorInfos[i].className == RegionDocumentProcessor::PROCESSOR_NAME) {
                auto iter = processorInfos[i].parameters.find(REGION_DISPATCHER_CONFIG);
                if (iter != processorInfos[i].parameters.end() && iter->second == ALL_REGION_DISPATCHER_TYPE) {
                    needRewrite = true;
                    regionProcessorIdx = i;
                }
            }
        }
        if (!needRewrite) {
            newChains.push_back(inChains[i]);
            continue;
        }

        IndexPartitionSchemaPtr schema = getIndexPartitionSchema(resourceReader->getResourceReader(), inChains[i]);
        if (!schema) {
            BS_LOG(ERROR, "get schema failed for processorChain %ld", i);
            return emptyChains;
        }
        size_t regionCount = schema->GetRegionCount();
        if (regionCount == 1) {
            newChains.push_back(inChains[i]);
            continue;
        }
        for (size_t regionId = 0; regionId < regionCount; ++regionId) {
            DocProcessorChainConfig newChainConfig = inChains[i];
            auto& regionProcessorInfo = newChainConfig.processorInfos[regionProcessorIdx];
            regionProcessorInfo.parameters[REGION_DISPATCHER_CONFIG] = REGION_ID_DISPATCHER_TYPE;
            regionProcessorInfo.parameters[REGION_ID_CONFIG] = StringUtil::toString(regionId);
            newChains.push_back(newChainConfig);
        }
    }
    return newChains;
}

IndexPartitionSchemaPtr Processor::getIndexPartitionSchema(const ResourceReaderPtr& resourceReaderPtr,
                                                           const DocProcessorChainConfig& docProcessorChainConfig) const
{
    const vector<string> clusterNames = docProcessorChainConfig.clusterNames;
    if (clusterNames.empty()) {
        BS_LOG(ERROR, "missing clusternames in DocProcessorChain config");
        return nullptr;
    }
    string clusterFileName = resourceReaderPtr->getClusterConfRelativePath(clusterNames[0]);
    string tableName;
    if (!resourceReaderPtr->getConfigWithJsonPath(clusterFileName, "cluster_config.table_name", tableName)) {
        string errorMsg = "get tableName from cluster_config.table_name failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return nullptr;
    }
    if (tableName.empty()) {
        string errorMsg = "get tableName from cluster_config.table_name failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return nullptr;
    }

    string schemaFileName = ResourceReader::getSchemaConfRelativePath(tableName);
    std::string schemaPath = PathUtil::JoinPath(resourceReaderPtr->getConfigPath(), schemaFileName);
    try {
        return IndexPartitionSchema::Load(schemaPath);
    } catch (const ExceptionBase& e) {
        string errorMsg = "load schema[" + tableName + "] failed, exception[" + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return nullptr;
    }
    return nullptr;
}

vector<string> Processor::getClusterNames(const DocProcessorChainConfig& chainConfig,
                                          const vector<string>& clusterNames)
{
    vector<string> processClusterNames;
    for (size_t i = 0; i < clusterNames.size(); i++) {
        if (chainConfig.clusterNames.end() !=
            find(chainConfig.clusterNames.begin(), chainConfig.clusterNames.end(), clusterNames[i])) {
            processClusterNames.push_back(clusterNames[i]);
        } else {
            BS_INTERVAL_LOG2(60, WARN, "cluster[%s] is not in processor_chain_config", clusterNames[i].c_str());
        }
    }
    return processClusterNames;
}

void Processor::stop(bool instant, bool seal)
{
    ScopedLock lock(_stopMtx);
    if (_stopped) {
        return;
    }
    _sealed = seal;
    if (_executor) {
        _executor->stop(instant);
    }
    _stopped = true;
}

void Processor::drop()
{
    ScopedLock lock(_stopMtx);
    if (_stopped) {
        return;
    }
    auto dropped = _dropped.exchange(true);
    if (dropped) {
        return;
    }
    if (_executor) {
        _executor->stop(/*instant*/ true);
    }
    _stopped = true;
}

void Processor::processDoc(const document::RawDocumentPtr& rawDoc)
{
    if (unlikely(isDropped())) {
        return;
    }
    ProcessorWorkItem* workItem =
        new ProcessorWorkItem(getProcessorChains(), _chainSelector, _enableRewriteDeleteSubDoc, &_reporter);
    workItem->setProcessErrorCounter(_processErrorCounter);
    workItem->setProcessDocCountCounter(_processDocCountCounter);
    workItem->initProcessData(rawDoc);
    if (!_executor->push(workItem)) {
        BS_LOG(WARN, "push work item failed, drop it");
        delete workItem;
    }
}

document::ProcessedDocumentVecPtr Processor::getProcessedDoc()
{
    while (true) {
        REPORT_METRIC(_waitProcessCountMetric, _executor->getWaitItemCount());
        ProcessorWorkItem* workItem = _executor->pop();
        if (!workItem) {
            break;
        }

        ProcessedDocumentVecPtr processedDocVecPtr = workItem->getProcessedDocs();
        delete workItem;
        if (processedDocVecPtr) {
            return processedDocVecPtr;
        }
    }
    return document::ProcessedDocumentVecPtr();
}

document::ProcessedDocumentVecPtr Processor::process(const document::RawDocumentVecPtr& rawDoc)
{
    ProcessorWorkItem workItem(getProcessorChains(), _chainSelector, false, &_reporter);
    workItem.setProcessErrorCounter(_processErrorCounter);
    workItem.setProcessDocCountCounter(_processDocCountCounter);
    workItem.initBatchProcessData(rawDoc);
    workItem.process();
    return workItem.getProcessedDocs();
}

DocumentProcessorChainVecPtr Processor::getProcessorChains() const
{
    ScopedLock lock(_chainMutex);
    return _chains;
}

void Processor::setProcessorChains(const DocumentProcessorChainVecPtr& newChains)
{
    ScopedLock lock(_chainMutex);
    _chains = newChains;
}

uint32_t Processor::getWaitProcessCount() const { return _executor->getWaitItemCount(); }

uint32_t Processor::getProcessedCount() const { return _executor->getOutputItemCount(); }

}} // namespace build_service::processor
