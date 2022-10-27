#include "build_service/workflow/SwiftBrokerFactory.h"
#include "build_service/util/FileUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "build_service/workflow/SwiftProcessedDocConsumer.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/common/PathDefine.h"
#include "build_service/processor/ProcessorConfig.h"
#include "build_service/common/CounterSynchronizerCreator.h"
#include "build_service/config/ProcessorConfigurator.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/util/SwiftAdminFacade.h"
#include <indexlib/util/counter/counter_map.h>
#include <autil/StringUtil.h>
#include <autil/legacy/jsonizable.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;

using SWIFT_NS(client)::SwiftClient;
using SWIFT_NS(client)::SwiftWriter;
using SWIFT_NS(client)::SwiftReader;
using SWIFT_NS(protocol)::Filter;

IE_NAMESPACE_USE(config);

using namespace build_service::proto;
using namespace build_service::util;
using namespace build_service::config;
using namespace build_service::common;

namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, SwiftBrokerFactory);

SwiftBrokerFactory::SwiftBrokerFactory(const SwiftClientCreatorPtr& swiftClientCreator)
    : _swiftClientCreator(swiftClientCreator)
{
}

SwiftBrokerFactory::~SwiftBrokerFactory() {
}

void SwiftBrokerFactory::stopProduceProcessedDoc() {
}

RawDocProducer *SwiftBrokerFactory::createRawDocProducer(const RoleInitParam &initParam) {
    return NULL;
}

RawDocConsumer *SwiftBrokerFactory::createRawDocConsumer(const RoleInitParam &initParam) {
    return NULL;
}

bool SwiftBrokerFactory::createSwiftReaderConfig(
        const RoleInitParam &initParam, const SwiftConfig& swiftConfig, string &swiftReadConfig)
{
    string clusterName = getClusterName(initParam);
    string topicPrefix = getValueFromKeyValueMap(initParam.kvMap, PROCESSED_DOC_SWIFT_TOPIC_PREFIX);
    string topicName;
    if (!SwiftAdminFacade::getTopicName(*(initParam.resourceReader),
                    topicPrefix,
                    initParam.partitionId.buildid(), clusterName,
                    initParam.partitionId.step() == BUILD_STEP_FULL, 
                    initParam.specifiedTopicId, topicName)) {
        BS_LOG(ERROR, "get cluster [%s] topic name failed", clusterName.c_str());
        return false;
    }
    
    BS_LOG(DEBUG, "processedDocTopicName: %s", topicName.c_str());

    const string& readerConfig = swiftConfig.getSwiftReaderConfig(initParam.partitionId.step());
    stringstream ss;
    ss << "topicName=" << topicName << ";";
    if (!readerConfig.empty()) {
        ss << readerConfig << ";";
    }
    ss << "from=" << initParam.partitionId.range().from() << ";"
       << "to=" << initParam.partitionId.range().to() << ";"
       << "uint8FilterMask=" << (uint32_t)initParam.swiftFilterMask << ";"
       << "uint8MaskResult=" << (uint32_t)initParam.swiftFilterResult;
    swiftReadConfig = ss.str();
    BS_LOG(INFO, "create swift reader with config %s", ss.str().c_str());
    return true;
}

ProcessedDocProducer *SwiftBrokerFactory::createProcessedDocProducer(
        const RoleInitParam &initParam)
{
    if (initParam.partitionId.clusternames_size() != 1) {
        string errorMsg = string("Fail to create swift reader : invalid clusternames: ") +
            initParam.partitionId.ShortDebugString() + ", should be one cluster id";
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_CLUSTER, errorMsg, BS_STOP);
        return NULL;
    }

    string clusterName = getClusterName(initParam);
    SwiftConfig swiftConfig;
    if (!initParam.resourceReader->getConfigWithJsonPath(
            ResourceReader::getClusterConfRelativePath(clusterName),
            "swift_config", swiftConfig))
    {
        string errorMsg = "parse swift_config for cluster[" + clusterName + "] failed";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }
    
    string zfsRootPath = getValueFromKeyValueMap(initParam.kvMap, PROCESSED_DOC_SWIFT_ROOT);
    
    const string& swiftClientConfig = swiftConfig.getSwiftClientConfig(initParam.partitionId.step());
    SwiftClient *client = createClient(zfsRootPath, swiftClientConfig);
    if (!client) {
        return NULL;
    }

    string swiftReaderConfig;
    if (!createSwiftReaderConfig(initParam, swiftConfig, swiftReaderConfig)) {
        return NULL;
    }
    SwiftReader *reader = createReader(client, swiftReaderConfig);
    if (!reader) {
        string errorMsg = string("Fail to create swift reader for config: ") + swiftReaderConfig;
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_SWIFT_CREATE_READER, errorMsg, BS_STOP);
        return NULL;
    }

    SwiftProcessedDocProducer *producer =
        doCreateProcessedDocProducer(reader, initParam);
    if (!initSwiftProcessedDocProducer(initParam, producer)) {
        DELETE_AND_SET_NULL(producer);
        return NULL;
    }
    return producer;
}

SwiftProcessedDocProducer *SwiftBrokerFactory::doCreateProcessedDocProducer(
    SWIFT_NS(client)::SwiftReader *reader, const RoleInitParam &param)
{
    string dataTable = param.partitionId.buildid().ShortDebugString();
    if (!param.schema)
    {
        string errorMsg = string("can not create SwiftProcessedDocProducer without schema");
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
    }
    return new SwiftProcessedDocProducer(reader, param.schema, dataTable);
}

bool SwiftBrokerFactory::initSwiftProcessedDocProducer(const RoleInitParam &initParam,
        SwiftProcessedDocProducer* producer)
{
    if (!producer) {
        return false;
    }
    int64_t startTimestamp = 0;
    KeyValueMap::const_iterator it = initParam.kvMap.find(CHECKPOINT);
    if (it != initParam.kvMap.end()) {
        if (!StringUtil::fromString<int64_t>(it->second, startTimestamp)) {
            BS_LOG(ERROR, "invalid checkpoint[%s]", it->second.c_str());
            return false;
        }
    }

    int64_t stopTimestamp = numeric_limits<int64_t>::max();
    it = initParam.kvMap.find(PROCESSED_DOC_SWIFT_STOP_TIMESTAMP);
    if (it != initParam.kvMap.end()) {
        if (!StringUtil::fromString(it->second, stopTimestamp)) {
            string errorMsg = "invalid stopTimestamp[" + it->second + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }

    string clusterName = getClusterName(initParam);
    SwiftConfig swiftConfig;
    if (!initParam.resourceReader->getConfigWithJsonPath(
            ResourceReader::getClusterConfRelativePath(clusterName),
            "swift_config", swiftConfig))
    {
        string errorMsg = "parse swift_config for cluster[" + clusterName + "] failed";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }

    const auto& fullTopicConfig = swiftConfig.getFullBrokerTopicConfig();
    int64_t noMoreMsgPeriod = fullTopicConfig->noMoreMsgPeriod;
    int64_t maxCommitInterval = fullTopicConfig->maxCommitInterval;

    // TODO: set document factory wrapper
    if (!producer->init(initParam.metricProvider,
                        initParam.resourceReader->getPluginPath(),
                        initParam.counterMap,
                        startTimestamp, stopTimestamp,
                        noMoreMsgPeriod, maxCommitInterval))
    {
        stringstream ss;
        ss << "init swift producer failed, startTimestamp: "
           << startTimestamp << ", stopTimestamp: " << stopTimestamp;
        REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_INIT_SWIFT_PRODUCER, ss.str(), BS_STOP);
        return false;
    }
    return true;
}

ProcessedDocConsumer *SwiftBrokerFactory::createProcessedDocConsumer(
        const RoleInitParam &initParam)
{
    string readerSrc = getValueFromKeyValueMap(initParam.kvMap, READ_SRC);
    if (readerSrc.empty()) {
        string errorMsg = "reader src can't be empty.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }

    processor::ProcessorConfig processorConfig;
    const string &dataTable = initParam.partitionId.buildid().datatable();
    if (!initParam.resourceReader->getDataTableConfigWithJsonPath(
                    dataTable, "processor_config", processorConfig))
    {
        string errorMsg = "parse processor_config for data table ["+ dataTable + "] failed";
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

    SwiftProcessedDocConsumer *consumer = new SwiftProcessedDocConsumer;
    if (!consumer->init(swiftWriters, initParam.counterSynchronizer,
                        processorConfig.checkpointInterval, srcSignature))
    {
        string errorMsg = "Init SwiftProcessedDocConsumer failed.";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        BEEPER_INTERVAL_REPORT(30, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        delete consumer;
        return NULL;
    }

    return consumer;
}

map<std::string, SwiftWriterWithMetric> SwiftBrokerFactory::createSwiftWriters(
        const RoleInitParam &initParam)
{
    map<std::string, SwiftWriterWithMetric> swiftWriters;
    string dataDescriptionStr = getValueFromKeyValueMap(
            initParam.kvMap, DATA_DESCRIPTION_KEY);

    if (dataDescriptionStr.empty()) {
        BS_LOG(ERROR, "Fail to create swift writers: dataDescription is not specified.");
        return map<std::string, SwiftWriterWithMetric>();
    }

    DataDescription dataDescription;
    try {
        FromJsonString(dataDescription, dataDescriptionStr);
    } catch (const ExceptionBase &e) {
        string errorMsg = "Invalid json string for dataDescription["
                          + dataDescriptionStr + "] : error ["
                          + string(e.what()) + "]";
        BS_LOG(ERROR, "Fail to create swift writers: %s", errorMsg.c_str());
        return map<std::string, SwiftWriterWithMetric>();
    } 
    
    string zfsRootPath = getValueFromKeyValueMap(
            initParam.kvMap, PROCESSED_DOC_SWIFT_ROOT);
    const PartitionId &pid = initParam.partitionId;
    if (pid.clusternames_size() == 0) {
        string errorMsg = "Fail to create swift writers : clusternames should not be empty.";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return map<std::string, SwiftWriterWithMetric>();
    }

    string topicPrefix = getValueFromKeyValueMap(
            initParam.kvMap, PROCESSED_DOC_SWIFT_TOPIC_PREFIX);

    for (int32_t i = 0; i < pid.clusternames_size(); ++i) {
        const string &clusterName = pid.clusternames(i);
        
        SwiftConfig swiftConfig;
        if(!initParam.resourceReader->getConfigWithJsonPath(
                ResourceReader::getClusterConfRelativePath(clusterName),
                "swift_config", swiftConfig))
        {
            string errorMsg = "Fail to parse swift_config for cluster : " + clusterName;
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return map<std::string, SwiftWriterWithMetric>();
        }
        SwiftClient *client = createClient(zfsRootPath,
                swiftConfig.getSwiftClientConfig(pid.step()));
        if (!client) {
            string errorMsg = "Fail to create swift writers : connect to swift failed.";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return map<std::string, SwiftWriterWithMetric>();
        }
        string topicPrefix = getValueFromKeyValueMap(
                initParam.kvMap, PROCESSED_DOC_SWIFT_TOPIC_PREFIX);
        string topicName;
        const ResourceReader& resourceReader = *(initParam.resourceReader);
        if (!SwiftAdminFacade::getTopicName(
                        resourceReader, topicPrefix,
                        pid.buildid(), clusterName,
                        pid.step() == BUILD_STEP_FULL,
                        initParam.specifiedTopicId, topicName))
        {
            string errorMsg = "Fail to get topicName : connect to swift failed.";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return map<std::string, SwiftWriterWithMetric>();
        }
        uint64_t swiftWriterMaxBufferSize = 0;

        if (!ProcessorConfigurator::getSwiftWriterMaxBufferSize(initParam.resourceReader,
                        pid, clusterName, dataDescription, swiftWriterMaxBufferSize))
        {
            string errorMsg = "Fail to get swift writer maxBufferSize for cluster : " + clusterName;
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);            
            return map<std::string, SwiftWriterWithMetric>();
        } else {
            BS_LOG(INFO, "set swift writer maxBufferSize[%lu] for cluster [%s]",
                   swiftWriterMaxBufferSize, clusterName.c_str());
        }

        string writerConfig = swiftConfig.getSwiftWriterConfig(
                swiftWriterMaxBufferSize, pid.step());
        BS_LOG(INFO, "processedDocTopicName: %s", topicName.c_str());
        SwiftWriterWithMetric oneWriter;
        oneWriter.swiftWriter.reset(createWriter(client, topicName, writerConfig));
        oneWriter.zkRootPath = zfsRootPath;
        oneWriter.topicName = topicName;
        if (!oneWriter.swiftWriter) {
            string errorMsg = string("Fail to create swift writer, topicName: ")+
                              topicName + ", config: " + writerConfig;
            REPORT_ERROR_WITH_ADVICE(BUILDFLOW_ERROR_SWIFT_CREATE_WRITER, errorMsg, BS_STOP);
            return map<std::string, SwiftWriterWithMetric>();
        }
        if (initParam.metricProvider) {
            oneWriter.unsendMsgSizeMetric = initParam.metricProvider->DeclareMetric(
                "debug/unsendMsgSize_" + clusterName, kmonitor::STATUS);
            oneWriter.uncommittedMsgSizeMetric = initParam.metricProvider->DeclareMetric(
                "debug/uncommittedMsgSize_" + clusterName, kmonitor::STATUS);
            oneWriter.sendBufferFullQpsMetric = initParam.metricProvider->DeclareMetric(
                "debug/sendBufferFullQps_" + clusterName, kmonitor::QPS);
        }
        swiftWriters[clusterName] = oneWriter;
    }
    return swiftWriters;
}


SwiftClient *SwiftBrokerFactory::createClient(
        const string &zfsRootPath,
        const string& swiftClientConfig) {
    return _swiftClientCreator->createSwiftClient(zfsRootPath, swiftClientConfig);
}

SwiftWriter *SwiftBrokerFactory::createWriter(
        SwiftClient* client,
        const string &topicName,
        const string &writerConfig)
{
    assert(client);
    string writerConfigStr = "topicName=" + topicName + ";" + writerConfig;
    return client->createWriter(writerConfigStr, NULL);
}

bool SwiftBrokerFactory::initCounterMap(RoleInitParam &initParam) {
    const KeyValueMap& kvMap = initParam.kvMap;

    auto it = kvMap.find(COUNTER_CONFIG_JSON_STR);
    if (it == kvMap.end()) {
        BS_LOG(ERROR, "[%s] missing in RoleInitParam", COUNTER_CONFIG_JSON_STR.c_str());
        return false;
    }

    CounterConfigPtr counterConfig(new CounterConfig());
    try {
        FromJsonString(*counterConfig.get(), it->second);
    } catch (const ExceptionBase &e) {
        BS_LOG(ERROR, "jsonize [%s] failed, original str : [%s]",
               COUNTER_CONFIG_JSON_STR.c_str(), it->second.c_str());
        return false;
    }

    bool loadFromExisted;
    IE_NAMESPACE(util)::CounterMapPtr counterMap =
        CounterSynchronizerCreator::loadCounterMap(counterConfig, loadFromExisted);

    if (!counterMap) {
        BS_LOG(ERROR, "load counter map failed");
        return false;
    }

    if (!loadFromExisted) {
        BS_LOG(INFO, "a new & empty counterMap is created!");
    }

    CounterSynchronizer* counterSynchronizer = CounterSynchronizerCreator::create(
            counterMap, counterConfig);

    if (!counterSynchronizer) {
        BS_LOG(ERROR, "create CounterSynchronizer failed");
        return false;
    }

    initParam.counterMap = counterMap;
    initParam.counterSynchronizer.reset(counterSynchronizer);
    return true;
}

string SwiftBrokerFactory::getClusterName(const RoleInitParam &initParam)
{
    if (initParam.partitionId.clusternames_size() == 0) {
        return string("");
    }

    assert(initParam.partitionId.clusternames_size() == 1);
    string clusterName = initParam.partitionId.clusternames(0);
    auto iter = initParam.kvMap.find(SHARED_TOPIC_CLUSTER_NAME);
    if (iter != initParam.kvMap.end()) {
        clusterName = iter->second;
    }
    return clusterName;
}

}
}
