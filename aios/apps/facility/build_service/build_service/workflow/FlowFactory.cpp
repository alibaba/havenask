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
#include "build_service/workflow/FlowFactory.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "build_service/builder/BuilderCreator.h"
#include "build_service/builder/BuilderCreatorV2.h"
#include "build_service/common/CounterSynchronizerCreator.h"
#include "build_service/common/ResourceKeeper.h"
#include "build_service/common/SwiftLinkFreshnessReporter.h"
#include "build_service/common/SwiftResourceKeeper.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/ProcessorConfig.h"
#include "build_service/config/TaskInputConfig.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/processor/BatchProcessor.h"
#include "build_service/reader/RawDocumentReaderCreator.h"
#include "build_service/util/RangeUtil.h"
#include "build_service/workflow/DocBuilderConsumer.h"
#include "build_service/workflow/DocBuilderConsumerV2.h"
#include "build_service/workflow/DocHandlerConsumer.h"
#include "build_service/workflow/DocHandlerProducer.h"
#include "build_service/workflow/DocProcessorConsumer.h"
#include "build_service/workflow/DocProcessorProducer.h"
#include "build_service/workflow/DocReaderProducer.h"
#include "build_service/workflow/MultiSwiftProcessedDocProducerV2.h"
#include "build_service/workflow/SingleSwiftProcessedDocProducerV2.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace indexlib::common;

using namespace std;
using namespace indexlib::util;
using namespace build_service::reader;
using namespace build_service::processor;
using namespace build_service::builder;
using namespace build_service::proto;
using namespace build_service::config;

namespace build_service { namespace workflow {
using common::ResourceKeeperPtr;
using common::SwiftResourceKeeper;
using common::SwiftResourceKeeperPtr;

BS_LOG_SETUP(workflow, FlowFactory);

FlowFactory::FlowFactory(const common::ResourceKeeperMap& resourceMap,
                         const util::SwiftClientCreatorPtr& swiftClientCreator,
                         const indexlib::partition::IndexPartitionPtr& indexPart,
                         const indexlib::util::TaskSchedulerPtr& taskScheduler)
    : _availableResources(resourceMap)
    , _swiftClientCreator(swiftClientCreator)
    , _indexPart(indexPart)
    , _readerCreator(swiftClientCreator)
    , _reader(NULL)
    , _docHandler(NULL)
    , _srcNode(NULL)
    , _processor(NULL)
    , _builder(NULL)
    , _builderv2(nullptr)
    , _taskScheduler(taskScheduler)
    , _isTablet(false)
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
}
FlowFactory::FlowFactory(const common::ResourceKeeperMap& resourceMap,
                         const util::SwiftClientCreatorPtr& swiftClientCreator,
                         std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                         const indexlib::util::TaskSchedulerPtr& taskScheduler)
    : _availableResources(resourceMap)
    , _swiftClientCreator(swiftClientCreator)
    , _tablet(std::move(tablet))
    , _readerCreator(swiftClientCreator)
    , _reader(NULL)
    , _docHandler(NULL)
    , _srcNode(NULL)
    , _processor(NULL)
    , _builder(NULL)
    , _builderv2(nullptr)
    , _taskScheduler(taskScheduler)
    , _isTablet(true)
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
}

FlowFactory::~FlowFactory()
{
    DELETE_AND_SET_NULL(_reader);
    DELETE_AND_SET_NULL(_docHandler);
    DELETE_AND_SET_NULL(_srcNode);
    DELETE_AND_SET_NULL(_processor);
    DELETE_AND_SET_NULL(_builder);
    DELETE_AND_SET_NULL(_builderv2);
}

void FlowFactory::collectErrorInfos(std::vector<ErrorInfo>& errorInfos) const
{
    fillErrorInfos(errorInfos);

    _readerCreator.fillErrorInfos(errorInfos);
}

RawDocProducer* FlowFactory::createRawDocProducer(const RoleInitParam& initParam, const ProducerType& type)
{
    assert(type == TP_RAW);
    reader::RawDocumentReader* reader = getReader(initParam);
    if (!reader) {
        return NULL;
    }
    string sourceSignatureStr = getValueFromKeyValueMap(initParam.kvMap, config::SRC_SIGNATURE);
    uint64_t srcSignature = 0;
    if (!autil::StringUtil::fromString<uint64_t>(sourceSignatureStr, srcSignature)) {
        BS_LOG(ERROR, "srcSignature [%s] illegal", sourceSignatureStr.c_str());
        return NULL;
    }
    return new DocReaderProducer(initParam.partitionId, initParam.resourceReader, reader, srcSignature);
}

RawDocConsumer* FlowFactory::createRawDocConsumer(const RoleInitParam& initParam, const ConsumerType& type)
{
    switch (type) {
    case TP_BUILDER:
        return createRawDocBuilderConsumer(initParam);
    case TP_PROCESSOR: {
        Processor* processor = getProcessor(initParam);
        if (!processor) {
            return NULL;
        }
        return new DocProcessorConsumer(processor);
    }
    default:
        BS_LOG(ERROR, "create raw doc consumer failed type [%d]", (int32_t)type);
        return NULL;
    }
}

ProcessedDocProducer* FlowFactory::createProcessedDocProducer(const RoleInitParam& initParam, const ProducerType& type)
{
    switch (type) {
    case TP_SRC: {
        ProcessedDocHandler* handler = getDocHandler(initParam);
        if (!handler) {
            return NULL;
        }
        return new DocHandlerProducer(handler);
    }
    case TP_PROCESSOR: {
        Processor* processor = getProcessor(initParam);
        if (!processor) {
            return NULL;
        }
        return new DocProcessorProducer(processor);
    }
    case TP_MESSAGE_QUEUE:
        return createSwiftProcessedDocProducer(initParam);
    default:
        BS_LOG(ERROR, "create processed  doc producer failed type [%d]", (int32_t)type);
        return NULL;
    }
}

ProcessedDocConsumer* FlowFactory::createProcessedDocConsumer(const RoleInitParam& initParam, const ConsumerType& type)
{
    switch (type) {
    case TP_SRC: {
        ProcessedDocHandler* handler = getDocHandler(initParam);
        if (!handler) {
            return NULL;
        }
        return new DocHandlerConsumer(handler);
    }
    case TP_BUILDER: {
        Builder* builder = getBuilder(initParam);
        if (!builder) {
            return NULL;
        }
        DocBuilderConsumer* consumer = new DocBuilderConsumer(builder);
        auto it = initParam.kvMap.find(config::BUILD_VERSION_TIMESTAMP);
        if (it != initParam.kvMap.end()) {
            int64_t buildVersionTimestamp = -1;
            if (!autil::StringUtil::fromString(it->second, buildVersionTimestamp)) {
                BS_LOG(ERROR, "invalid build version timestamp[%s]", it->second.c_str());
                return NULL;
            } else {
                consumer->SetEndBuildTimestamp(buildVersionTimestamp);
            }
        }
        return consumer;
    }
    case TP_BUILDERV2: {
        BuilderV2* builder = getBuilderV2(initParam);
        if (!builder) {
            return NULL;
        }
        DocBuilderConsumerV2* consumer = new DocBuilderConsumerV2(builder);
        auto it = initParam.kvMap.find(config::BUILD_VERSION_TIMESTAMP);
        if (it != initParam.kvMap.end()) {
            int64_t buildVersionTimestamp = -1;
            if (!autil::StringUtil::fromString(it->second, buildVersionTimestamp)) {
                BS_LOG(ERROR, "invalid build version timestamp[%s]", it->second.c_str());
                return NULL;
            } else {
                consumer->SetEndBuildTimestamp(buildVersionTimestamp);
            }
        }
        return consumer;
    }
    case TP_MESSAGE_QUEUE:
        return createSwiftProcessedDocConsumer(initParam);
    default:
        BS_LOG(ERROR, "create processed  doc consumer failed type [%d]", (int32_t)type);
        return NULL;
    }
}

RawDocConsumer* FlowFactory::createRawDocBuilderConsumer(const RoleInitParam& initParam)
{
    Builder* builder = getBuilder(initParam);
    if (!builder) {
        return NULL;
    }
    Processor* processor = nullptr;
    if (needProcessRawdoc(initParam)) {
        processor = getProcessor(initParam);
        if (!processor) {
            BS_LOG(ERROR, "get processor failed");
            return nullptr;
        }
    }
    RawDocConsumer* consumer = new RawDocBuilderConsumer(builder, processor);
    auto it = initParam.kvMap.find(config::BUILD_VERSION_TIMESTAMP);
    if (it != initParam.kvMap.end()) {
        int64_t buildVersionTimestamp = -1;
        if (!autil::StringUtil::fromString(it->second, buildVersionTimestamp)) {
            BS_LOG(ERROR, "invalid build version timestamp[%s]", it->second.c_str());
            return NULL;
        } else {
            consumer->SetEndBuildTimestamp(buildVersionTimestamp);
        }
    }
    return consumer;
}

SwiftProcessedDocProducer* FlowFactory::doCreateSwiftProcessedDocProducer(const common::SwiftParam& swiftParam,
                                                                          const RoleInitParam& param)
{
    string dataTable = param.partitionId.buildid().ShortDebugString();
    if (param.isTablet) {
        if (!param.schemav2) {
            string errorMsg = string("can not create SwiftProcessedDocProducer without schema");
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        }
        return new SingleSwiftProcessedDocProducerV2(swiftParam, param.schemav2, param.partitionId, _taskScheduler);
    } else {
        if (!param.schema) {
            string errorMsg = string("can not create SwiftProcessedDocProducer without schema");
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        }
        return new SingleSwiftProcessedDocProducer(swiftParam, param.schema, param.partitionId, _taskScheduler);
    }
}

ProcessedDocProducer* FlowFactory::createSwiftProcessedDocProducer(const RoleInitParam& initParam)
{
    if (initParam.partitionId.clusternames_size() != 1) {
        string errorMsg = string("Fail to create swift reader : invalid clusternames: ") +
                          initParam.partitionId.ShortDebugString() + ", should be one cluster id";
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CLUSTER, errorMsg, BS_STOP);
        return NULL;
    }
    string clusterName = initParam.partitionId.clusternames(0);
    string inputInfo = getValueFromKeyValueMap(initParam.kvMap, clusterInputKey(clusterName));
    config::TaskInputConfig inputConfig;
    try {
        FromJsonString(inputConfig, inputInfo);
    } catch (const autil::legacy::ExceptionBase& e) {
        string errorMsg = "Invalid json string for input info[" + inputInfo + "] : error [" + string(e.what()) + "]";
        return nullptr;
    }
    if (inputConfig.getType() != "dependResource") {
        BS_LOG(ERROR, "not support input type [%s]", inputConfig.getType().c_str());
        return NULL;
    }

    string inputResourceName = getValueFromKeyValueMap(inputConfig.getParameters(), "resourceName");
    ResourceKeeperPtr& keeper = _availableResources[inputResourceName];
    if (!keeper) {
        BS_LOG(ERROR, "get resource failed with resource name  [%s]", inputResourceName.c_str());
        return NULL;
    }
    _usingResources[inputResourceName] = keeper;

    assert(keeper->getType() == "swift");
    SwiftResourceKeeperPtr swiftKeeper = DYNAMIC_POINTER_CAST(SwiftResourceKeeper, keeper);
    SwiftConfig swiftConfig;
    if (!swiftKeeper->getSwiftConfig(initParam.resourceReader, swiftConfig)) {
        string errorMsg = "parse swift_config for topic[" + swiftKeeper->getTopicName() + "] failed";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }

    uint32_t produceDocThreadCount = 1;
    initParam.resourceReader->getConfigWithJsonPath(ResourceReader::getClusterConfRelativePath(clusterName),
                                                    "build_option_config.read_processed_document_thread_count",
                                                    produceDocThreadCount);
    produceDocThreadCount = autil::EnvUtil::getEnv("READ_PROCESSED_DOCUMENT_THREAD_COUNT", produceDocThreadCount);
    if (produceDocThreadCount <= 1) {
        return createSingleSwiftProcessedDocProducer(initParam, swiftKeeper, swiftConfig);
    } else {
        return createMultiSwiftProcessedDocProducer(produceDocThreadCount, initParam, swiftKeeper, swiftConfig);
    }
}

SwiftProcessedDocProducer* FlowFactory::createSingleSwiftProcessedDocProducer(const RoleInitParam& initParam,
                                                                              const SwiftResourceKeeperPtr& swiftKeeper,
                                                                              const SwiftConfig& swiftConfig)
{
    auto swiftParam = swiftKeeper->createSwiftReader(_swiftClientCreator, initParam.kvMap, initParam.resourceReader,
                                                     initParam.partitionId.range());
    if (unlikely(!swiftParam.reader)) {
        string errorMsg = string("Fail to create swift reader " + initParam.partitionId.ShortDebugString());
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_SWIFT_CREATE_READER, errorMsg, BS_STOP);
        return NULL;
    }

    std::string topicName = swiftParam.topicName;
    swift::network::SwiftAdminAdapterPtr swiftAdapter;
    swift::client::SwiftClient* client = swiftKeeper->createSwiftClient(_swiftClientCreator, initParam.resourceReader);
    if (client) {
        swiftAdapter = client->getAdminAdapter();
    }
    int64_t totalSwiftPartitionCount = -1;
    if (swiftAdapter == nullptr) {
        BS_LOG(ERROR, "no valid swift adapter for topic [%s]", topicName.c_str());
    } else {
        if (!util::RangeUtil::getPartitionCount(swiftAdapter.get(), topicName, &totalSwiftPartitionCount)) {
            BS_LOG(ERROR, "failed to get swift partition count for topic [%s]", topicName.c_str());
        }
    }

    SwiftProcessedDocProducer* producer = doCreateSwiftProcessedDocProducer(swiftParam, initParam);
    if (!initSwiftProcessedDocProducer(swiftConfig, initParam, topicName, producer)) {
        DELETE_AND_SET_NULL(producer);
        return NULL;
    }

    if (!initSwiftLinkReporter(initParam, totalSwiftPartitionCount, topicName, producer)) {
        string errorMsg = "create swift link reporter for topic [" + topicName + "] failed";
        BEEPER_INTERVAL_REPORT(30, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
    }
    return producer;
}

SwiftProcessedDocProducer* FlowFactory::createMultiSwiftProcessedDocProducer(uint16_t produceDocThreadCount,
                                                                             const RoleInitParam& initParam,
                                                                             const SwiftResourceKeeperPtr& swiftKeeper,
                                                                             const SwiftConfig& swiftConfig)
{
    assert(produceDocThreadCount >= 2);

    swift::network::SwiftAdminAdapterPtr swiftAdapter;
    swift::client::SwiftClient* client = swiftKeeper->createSwiftClient(_swiftClientCreator, initParam.resourceReader);
    if (client) {
        swiftAdapter = client->getAdminAdapter();
    }
    int64_t totalSwiftPartitionCount = -1;
    const std::string& topicName = swiftKeeper->getTopicName();
    if (swiftAdapter == nullptr) {
        BS_LOG(ERROR, "no valid swift adapter for topic [%s]", topicName.c_str());
    } else {
        if (!util::RangeUtil::getPartitionCount(swiftAdapter.get(), topicName, &totalSwiftPartitionCount)) {
            BS_LOG(ERROR, "failed to get swift partition count for topic [%s]", topicName.c_str());
        }
    }

    std::vector<proto::Range> ranges;
    if (totalSwiftPartitionCount > 0 and (RANGE_MAX + 1) % totalSwiftPartitionCount == 0) {
        ranges = util::RangeUtil::splitRangeBySwiftPartition(initParam.partitionId.range().from(),
                                                             initParam.partitionId.range().to(),
                                                             (uint32_t)totalSwiftPartitionCount,
                                                             /*parallelNum=*/produceDocThreadCount);
    } else {
        // fallback to split by parallel thread count
        BS_LOG(ERROR,
               "Fallback to read swift splitted by parallel read thread count, multiple threads might read same swift "
               "partition[%u]",
               produceDocThreadCount);
        ranges = util::RangeUtil::splitRange(initParam.partitionId.range().from(), initParam.partitionId.range().to(),
                                             produceDocThreadCount);
    }

    std::vector<common::SwiftParam> params;
    params.reserve(produceDocThreadCount);
    for (const proto::Range& range : ranges) {
        auto param =
            swiftKeeper->createSwiftReader(_swiftClientCreator, initParam.kvMap, initParam.resourceReader, range);

        if (unlikely(!param.reader)) {
            string errorMsg = string("Fail to create swift reader " + initParam.partitionId.ShortDebugString());
            REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_SWIFT_CREATE_READER, errorMsg, BS_STOP);
            return NULL;
        }
        params.push_back(param);
    }

    SwiftProcessedDocProducer* producer = nullptr;
    if (initParam.isTablet) {
        if (unlikely(!initParam.schemav2)) {
            string errorMsg = string("can not create SwiftProcessedDocProducer without schema");
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        }
        producer =
            new MultiSwiftProcessedDocProducerV2(params, initParam.schemav2, initParam.partitionId, _taskScheduler);
    } else {
        if (unlikely(!initParam.schema)) {
            string errorMsg = string("can not create SwiftProcessedDocProducer without schema");
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        }
        producer = new MultiSwiftProcessedDocProducer(params, initParam.schema, initParam.partitionId, _taskScheduler);
    }
    if (!initSwiftProcessedDocProducer(swiftConfig, initParam, swiftKeeper->getTopicName(), producer)) {
        DELETE_AND_SET_NULL(producer);
        return NULL;
    }

    if (!initSwiftLinkReporter(initParam, totalSwiftPartitionCount, swiftKeeper->getTopicName(), producer)) {
        string errorMsg = "create swift link reporter for topic [" + swiftKeeper->getTopicName() + "] failed";
        BEEPER_INTERVAL_REPORT(30, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
    }
    return producer;
}

bool FlowFactory::initSwiftProcessedDocProducer(const SwiftConfig& swiftConfig, const RoleInitParam& initParam,
                                                const string& topicName, SwiftProcessedDocProducer* producer)
{
    if (!producer) {
        return false;
    }
    int64_t startTimestamp = 0;
    KeyValueMap::const_iterator it = initParam.kvMap.find(CHECKPOINT);
    if (it != initParam.kvMap.end()) {
        if (!autil::StringUtil::fromString<int64_t>(it->second, startTimestamp)) {
            BS_LOG(ERROR, "invalid checkpoint[%s]", it->second.c_str());
            return false;
        }
    }

    int64_t stopTimestamp = numeric_limits<int64_t>::max();
    it = initParam.kvMap.find(PROCESSED_DOC_SWIFT_STOP_TIMESTAMP);
    if (it != initParam.kvMap.end()) {
        if (!autil::StringUtil::fromString(it->second, stopTimestamp)) {
            string errorMsg = "invalid stopTimestamp[" + it->second + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }

    uint64_t srcSignature = startTimestamp;
    it = initParam.kvMap.find(config::SRC_SIGNATURE);
    if (it != initParam.kvMap.end()) {
        string sourceSignatureStr = it->second;
        if (!autil::StringUtil::fromString<uint64_t>(sourceSignatureStr, srcSignature)) {
            BS_LOG(INFO, "srcSignature [%s] illegal", sourceSignatureStr.c_str());
            return false;
        }
    }
    const auto& fullTopicConfig = swiftConfig.getFullBrokerTopicConfig();
    int64_t noMoreMsgPeriod = fullTopicConfig->noMoreMsgPeriod;
    int64_t maxCommitInterval = fullTopicConfig->maxCommitInterval;
    bool allowSeekCrossSrc = (KV_TRUE == getValueFromKeyValueMap(initParam.kvMap, ALLOW_SEEK_CROSS_SRC, KV_FALSE));
    // TODO: set document factory wrapper
    if (!producer->init(initParam.metricProvider, initParam.resourceReader->getPluginPath(), initParam.counterMap,
                        startTimestamp, stopTimestamp, noMoreMsgPeriod, maxCommitInterval, srcSignature,
                        allowSeekCrossSrc)) {
        stringstream ss;
        ss << "init swift producer failed, startTimestamp: " << startTimestamp << ", stopTimestamp: " << stopTimestamp;
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_INIT_SWIFT_PRODUCER, ss.str(), BS_STOP);
        return false;
    }
    return true;
}

bool FlowFactory::initSwiftLinkReporter(const RoleInitParam& initParam, int64_t totalSwiftPartitionCount,
                                        const string& topicName, SwiftProcessedDocProducer* producer)
{
    if (!common::SwiftLinkFreshnessReporter::needReport()) {
        return true;
    }

    common::SwiftLinkFreshnessReporterPtr reporter(new common::SwiftLinkFreshnessReporter(initParam.partitionId));
    if (!reporter->init(initParam.metricProvider, totalSwiftPartitionCount, topicName)) {
        BS_LOG(ERROR, "init swift link reporter fail for topic [%s].", topicName.c_str());
        return false;
    }
    producer->setLinkReporter(reporter);
    return true;
}

map<string, SwiftWriterWithMetric> FlowFactory::createSwiftWriters(const RoleInitParam& initParam)
{
    map<std::string, SwiftWriterWithMetric> swiftWriters;
    string dataDescriptionStr = getValueFromKeyValueMap(initParam.kvMap, DATA_DESCRIPTION_KEY);

    if (dataDescriptionStr.empty()) {
        BS_LOG(ERROR, "Fail to create swift writers: dataDescription is not specified.");
        return map<std::string, SwiftWriterWithMetric>();
    }

    DataDescription dataDescription;
    try {
        FromJsonString(dataDescription, dataDescriptionStr);
    } catch (const ExceptionBase& e) {
        string errorMsg =
            "Invalid json string for dataDescription[" + dataDescriptionStr + "] : error [" + string(e.what()) + "]";
        BS_LOG(ERROR, "Fail to create swift writers: %s", errorMsg.c_str());
        return map<std::string, SwiftWriterWithMetric>();
    }

    const PartitionId& pid = initParam.partitionId;
    if (pid.clusternames_size() == 0) {
        string errorMsg = "Fail to create swift writers : clusternames should not be empty.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return map<std::string, SwiftWriterWithMetric>();
    }
    for (int32_t i = 0; i < pid.clusternames_size(); ++i) {
        const string& clusterName = pid.clusternames(i);
        string outputInfo = getValueFromKeyValueMap(initParam.kvMap, clusterOutputKey(clusterName));
        config::TaskOutputConfig outputConfig;
        try {
            FromJsonString(outputConfig, outputInfo);
        } catch (const autil::legacy::ExceptionBase& e) {
            string errorMsg =
                "Invalid json string for output info[" + outputInfo + "] : error [" + string(e.what()) + "]";
            BS_LOG(ERROR, "[%s]", errorMsg.c_str());
            return {};
        }
        SwiftWriterWithMetric oneWriter;
        if (outputConfig.getType() == "dependResource") {
            string outputKey = getValueFromKeyValueMap(outputConfig.getParameters(), "resourceName");
            ResourceKeeperPtr& keeper = _availableResources[outputKey];
            _usingResources[outputKey] = keeper;
            assert(keeper->getType() == "swift");
            SwiftResourceKeeperPtr swiftKeeper = DYNAMIC_POINTER_CAST(SwiftResourceKeeper, keeper);
            oneWriter.swiftWriter.reset(
                swiftKeeper->createSwiftWriter(_swiftClientCreator, initParam.partitionId.buildid().datatable(),
                                               dataDescription, initParam.kvMap, initParam.resourceReader));
            oneWriter.zkRootPath = swiftKeeper->getSwiftRoot();
            oneWriter.topicName = swiftKeeper->getTopicName();
            if (!oneWriter.swiftWriter) {
                string errorMsg = string("Fail to create swift writer, topicName: ") + swiftKeeper->getTopicName();
                REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_SWIFT_CREATE_WRITER, errorMsg, BS_STOP);
                return map<std::string, SwiftWriterWithMetric>();
            }
        } else {
            BS_LOG(ERROR, "unsupported output type [%s]", outputConfig.getType().c_str());
            return {};
        }

        if (initParam.metricProvider) {
            oneWriter.unsendMsgSizeMetric =
                initParam.metricProvider->DeclareMetric("debug/unsendMsgSize_" + clusterName, kmonitor::STATUS);
            oneWriter.uncommittedMsgSizeMetric =
                initParam.metricProvider->DeclareMetric("debug/uncommittedMsgSize_" + clusterName, kmonitor::STATUS);
            oneWriter.sendBufferFullQpsMetric =
                initParam.metricProvider->DeclareMetric("debug/sendBufferFullQps_" + clusterName, kmonitor::QPS);
        }
        swiftWriters[clusterName] = oneWriter;
    }
    return swiftWriters;
}

ProcessedDocConsumer* FlowFactory::createSwiftProcessedDocConsumer(const RoleInitParam& initParam)
{
    string readerSrc = getValueFromKeyValueMap(initParam.kvMap, READ_SRC);
    if (readerSrc.empty()) {
        string errorMsg = "reader src can't be empty.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }

    config::ProcessorConfig processorConfig;
    const string& dataTable = initParam.partitionId.buildid().datatable();
    ProcessorConfigReader reader(initParam.resourceReader, dataTable,
                                 getValueFromKeyValueMap(initParam.kvMap, config::PROCESSOR_CONFIG_NODE));

    if (!reader.getProcessorConfig("processor_config", &processorConfig)) {
        string errorMsg = "parse processor_config for data table [" + dataTable + "] failed";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }

    auto swiftWriters = createSwiftWriters(initParam);
    if (swiftWriters.empty()) {
        string errorMsg = "create swift writers failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        BEEPER_INTERVAL_REPORT(30, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        return NULL;
    }

    string sourceSignatureStr = getValueFromKeyValueMap(initParam.kvMap, config::SRC_SIGNATURE);
    uint64_t srcSignature = 0;
    if (!autil::StringUtil::fromString<uint64_t>(sourceSignatureStr, srcSignature)) {
        BS_LOG(INFO, "srcSignature [%s] illegal", sourceSignatureStr.c_str());
        ERROR_COLLECTOR_LOG(INFO, "srcSignature [%s] illegal", sourceSignatureStr.c_str());
        string msg = "srcSignature [" + sourceSignatureStr + "] illegal";
        BEEPER_INTERVAL_REPORT(30, WORKER_ERROR_COLLECTOR_NAME, msg);
        return NULL;
    }

    SwiftProcessedDocConsumer* consumer = new SwiftProcessedDocConsumer;
    int64_t startTimestamp = -1;
    KeyValueMap::const_iterator it = initParam.kvMap.find(CONSUMER_CHECKPOINT);
    if (it != initParam.kvMap.end()) {
        if (!autil::StringUtil::fromString<int64_t>(it->second, startTimestamp)) {
            BS_LOG(ERROR, "invalid checkpoint[%s]", it->second.c_str());
            return NULL;
        }
    }
    int32_t batchMask =
        getValueFromKeyValueMap(initParam.kvMap, IS_BATCH_MODE, KV_FALSE) == KV_TRUE
            ? autil::StringUtil::fromString<int32_t>(getValueFromKeyValueMap(initParam.kvMap, SWIFT_FILTER_RESULT))
            : -1;
    bool syncLocatorFromCounter =
        getValueFromKeyValueMap(initParam.kvMap, SYNC_LOCATOR_FROM_COUNTER, KV_TRUE) == KV_TRUE;
    common::Locator locator(srcSignature, startTimestamp);
    locator.SetUserData(getValueFromKeyValueMap(initParam.kvMap, config::LOCATOR_USER_DATA));
    if (!consumer->init(swiftWriters, initParam.counterSynchronizer, processorConfig.checkpointInterval, locator,
                        batchMask, /*disable counter = */ !syncLocatorFromCounter)) {
        string errorMsg = "Init SwiftProcessedDocConsumer failed.";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        BEEPER_INTERVAL_REPORT(30, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        delete consumer;
        return NULL;
    }

    return consumer;
}

reader::RawDocumentReader* FlowFactory::getReader(const RoleInitParam& initParam)
{
    if (_reader) {
        return _reader;
    }
    _reader = createReader(initParam);
    if (!_reader) {
        string errorMsg = "create reader failed, kvMap: " + keyValueMapToString(initParam.kvMap);
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CREATE_READER, errorMsg, BS_STOP);
    }
    return _reader;
}

reader::RawDocumentReader* FlowFactory::createReader(const RoleInitParam& initParam)
{
    if (initParam.schemav2) {
        return _readerCreator.create(initParam.resourceReader, initParam.kvMap, initParam.partitionId,
                                     initParam.metricProvider, initParam.counterMap, initParam.schemav2);
    }
    return _readerCreator.create(initParam.resourceReader, initParam.kvMap, initParam.partitionId,
                                 initParam.metricProvider, initParam.counterMap, initParam.schema);
}

processor::Processor* FlowFactory::getProcessor(const RoleInitParam& initParam)
{
    if (_processor) {
        return _processor;
    }
    _processor = createProcessor(initParam);
    if (!_processor) {
        string errorMsg = "create processor failed, kvMap: " + keyValueMapToString(initParam.kvMap);
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CREATE_PROCESSOR, errorMsg, BS_STOP);
    }
    return _processor;
}

builder::Builder* FlowFactory::getBuilder(const RoleInitParam& initParam)
{
    if (_builder) {
        return _builder;
    }
    _builder = createBuilder(initParam);
    if (!_builder) {
        string errorMsg = "create builder failed, kvMap: " + keyValueMapToString(initParam.kvMap);
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CREATE_BUILDER, errorMsg, BS_STOP);
    }
    return _builder;
}

builder::Builder* FlowFactory::createBuilder(const RoleInitParam& initParam)
{
    BuilderCreator creator(_indexPart);
    return creator.create(initParam.resourceReader, initParam.kvMap, initParam.partitionId, initParam.metricProvider);
}

builder::BuilderV2* FlowFactory::getBuilderV2(const RoleInitParam& initParam)
{
    if (_builderv2) {
        return _builderv2;
    }
    _builderv2 = createBuilderV2(initParam);
    if (!_builderv2) {
        string errorMsg = "create builder failed, kvMap: " + keyValueMapToString(initParam.kvMap);
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CREATE_BUILDER, errorMsg, BS_STOP);
    }
    return _builderv2;
}

builder::BuilderV2* FlowFactory::createBuilderV2(const RoleInitParam& initParam)
{
    BuilderCreatorV2 creator(_tablet);
    return creator.create(initParam.resourceReader, initParam.kvMap, initParam.partitionId, initParam.metricProvider)
        .release();
}

processor::Processor* FlowFactory::createProcessor(const RoleInitParam& initParam)
{
    ProcessorConfigReaderPtr reader(
        new ProcessorConfigReader(initParam.resourceReader, initParam.partitionId.buildid().datatable(),
                                  getValueFromKeyValueMap(initParam.kvMap, config::PROCESSOR_CONFIG_NODE)));
    ProcessorConfig processorConfig;
    if (!reader->getProcessorConfig("processor_config", &processorConfig)) {
        string errorMsg = "Get processor_config from [" + initParam.partitionId.buildid().datatable() + "] failed.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }

    processor::Processor* processor = NULL;
    if (processorConfig.processorStrategyStr == ProcessorConfig::NORMAL_PROCESSOR_STRATEGY) {
        processor = new Processor(processorConfig.processorStrategyParameter);
    } else if (processorConfig.processorStrategyStr == ProcessorConfig::BATCH_PROCESSOR_STRATEGY) {
        processor = new BatchProcessor(processorConfig.processorStrategyParameter);
    } else {
        string errorMsg =
            "unsupported processor_strategy [" + processorConfig.processorStrategyStr + "] in processor_config.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }

    if (!processor->start(reader, initParam.partitionId, initParam.metricProvider, initParam.counterMap,
                          initParam.kvMap, /*forceSingleThreaded*/ false, initParam.isTablet)) {
        delete processor;
        return NULL;
    }
    return processor;
}

SrcDataNode* FlowFactory::getSrcNode(const RoleInitParam& initParam)
{
    if (_srcNode) {
        return _srcNode;
    }
    _srcNode = createSrcNode(initParam);
    if (!_srcNode) {
        string errorMsg = "create src node failed, kvMap: " + keyValueMapToString(initParam.kvMap);
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CREATE_SRC_NODE, errorMsg, BS_STOP);
    }
    return _srcNode;
}

SrcDataNode* FlowFactory::createSrcNode(const RoleInitParam& param)
{
    SrcNodeConfig srcConfig;
    ProcessorConfigReaderPtr reader(
        new ProcessorConfigReader(param.resourceReader, param.partitionId.buildid().datatable(),
                                  getValueFromKeyValueMap(param.kvMap, config::PROCESSOR_CONFIG_NODE)));
    if (!SrcDataNode::readSrcConfig(reader, srcConfig)) {
        string errorMsg = "read src config failed";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return nullptr;
    }
    SrcDataNode* srcNode = new SrcDataNode(srcConfig, param.metricProvider);
    string srcKey = param.partitionId.clusternames(0) + SRC_RESOURCE_NAME;
    if (!srcNode->init(param.resourceReader, param.partitionId, _availableResources[srcKey])) {
        delete srcNode;
        return nullptr;
    }
    _usingResources[srcKey] = _availableResources[srcKey];
    return srcNode;
}

ProcessedDocHandler* FlowFactory::getDocHandler(const RoleInitParam& initParam)
{
    if (_docHandler) {
        return _docHandler;
    }
    _docHandler = createDocHandler(initParam);
    if (!_docHandler) {
        string errorMsg = "create doc handler  failed, kvMap: " + keyValueMapToString(initParam.kvMap);
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CREATE_DOC_HANDLER, errorMsg, BS_STOP);
    }
    return _docHandler;
}

ProcessedDocHandler* FlowFactory::createDocHandler(const RoleInitParam& param)
{
    processor::Processor* processor = getProcessor(param);
    if (!processor) {
        return nullptr;
    }
    SrcDataNode* srcNode = getSrcNode(param);
    if (!srcNode) {
        return nullptr;
    }
    ProcessedDocHandler* docHandle = new ProcessedDocHandler(srcNode, processor);
    int64_t consumerCheckpoint = 0;
    KeyValueMap::const_iterator it = param.kvMap.find(CONSUMER_CHECKPOINT);
    if (it != param.kvMap.end()) {
        if (!autil::StringUtil::fromString<int64_t>(it->second, consumerCheckpoint)) {
            BS_LOG(ERROR, "invalid checkpoint[%s]", it->second.c_str());
            return nullptr;
        }
    }
    if (!docHandle->start(param.resourceReader, param.kvMap, param.partitionId, param.metricProvider, param.counterMap,
                          param.schema, consumerCheckpoint)) {
        return nullptr;
    }
    return docHandle;
}

void FlowFactory::collectUsingResources(common::ResourceKeeperMap& usingKeepers)
{
    // collect latest using resources
    ResourceKeeperPtr resource;
    if (_srcNode) {
        resource = _srcNode->getUsingResource();
        if (resource) {
            _usingResources[resource->getResourceName()] = resource;
        }
    }
    for (auto it : _usingResources) {
        usingKeepers[it.first] = it.second;
    }
}

void FlowFactory::updateUsingResources(common::ResourceKeeperMap& availKeepers, bool& needRestart)
{
    _availableResources = availKeepers;
    // TODO xiaohao.yxh support update swift resource
    if (_srcNode) {
        bool srcNeedRestart = false;
        ResourceKeeperPtr resource;
        resource = _srcNode->getUsingResource();
        if (resource) {
            string resourceName = resource->getResourceName();
            _srcNode->updateResource(availKeepers[resourceName], srcNeedRestart);
            needRestart |= srcNeedRestart;
        }
    }
}

bool FlowFactory::initCounterMap(RoleInitParam& initParam, BuildFlowMode mode)
{
    if (mode & BuildFlowMode::BUILDER) {
        if (_isTablet) {
            auto builderV2 = getBuilderV2(initParam);
            if (!builderV2) {
                return false;
            }
            initParam.counterMap = builderV2->GetCounterMap();
        } else {
            builder::Builder* builder = getBuilder(initParam);
            if (!builder) {
                return false;
            }
            initParam.counterMap = builder->GetCounterMap();
        }
        return initParam.counterMap.get();
    } else {
        const KeyValueMap& kvMap = initParam.kvMap;

        auto it = kvMap.find(COUNTER_CONFIG_JSON_STR);
        if (it == kvMap.end()) {
            BS_LOG(ERROR, "[%s] missing in RoleInitParam", COUNTER_CONFIG_JSON_STR.c_str());
            return false;
        }

        config::CounterConfigPtr counterConfig(new config::CounterConfig());
        try {
            FromJsonString(*counterConfig.get(), it->second);
        } catch (const ExceptionBase& e) {
            BS_LOG(ERROR, "jsonize [%s] failed, original str : [%s]", COUNTER_CONFIG_JSON_STR.c_str(),
                   it->second.c_str());
            return false;
        }

        bool loadFromExisted;
        indexlib::util::CounterMapPtr counterMap =
            common::CounterSynchronizerCreator::loadCounterMap(counterConfig, loadFromExisted);

        if (!counterMap) {
            BS_LOG(ERROR, "load counter map failed");
            return false;
        }

        if (!loadFromExisted) {
            BS_LOG(INFO, "a new & empty counterMap is created!");
        }

        common::CounterSynchronizer* counterSynchronizer =
            common::CounterSynchronizerCreator::create(counterMap, counterConfig);

        if (!counterSynchronizer) {
            BS_LOG(ERROR, "create CounterSynchronizer failed");
            return false;
        }

        initParam.counterMap = counterMap;
        initParam.counterSynchronizer.reset(counterSynchronizer);
        return true;
    }
}

bool FlowFactory::needProcessRawdoc(const RoleInitParam& initParam)
{
    auto it = initParam.kvMap.find(config::REALTIME_PROCESS_RAWDOC);
    if (it != initParam.kvMap.end() && it->second == "true") {
        return true;
    }
    return false;
}

}} // namespace build_service::workflow
