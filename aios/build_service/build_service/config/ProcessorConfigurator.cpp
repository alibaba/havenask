#include "build_service/config/ProcessorConfigurator.h"
#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/BuildRuleConfig.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::proto;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, ProcessorConfigurator);

ProcessorConfigurator::ProcessorConfigurator() {
}

ProcessorConfigurator::~ProcessorConfigurator() {
}

const int64_t ProcessorConfigurator::DEFAULT_WRITER_BUFFER_SIZE = 512 * 1024 * 1024; // 512MB

bool ProcessorConfigurator::getSwiftWriterMaxBufferSize(
        const ResourceReaderPtr& resourceReader,
        const PartitionId& pid,
        const string& clusterName,
        const DataDescription& dataDescription,
        uint64_t& maxBufferSize)
{
    if (!resourceReader) {
        BS_LOG(ERROR, "pid[%s] cluster[%s] getSwiftWriterMaxBufferSize failed,"
               " resource reader is null", pid.ShortDebugString().c_str(), clusterName.c_str());

        return false;
    }

    SwiftConfig swiftConfig;
    if (!getConfig(resourceReader,
                   ResourceReader::getClusterConfRelativePath(clusterName),
                   "swift_config",
                   swiftConfig))
    {
        return false;
    }

    string dataTableName = pid.buildid().datatable();
    uint64_t customizeWriterBufferSize = 0;
    if (getCustomizeWriterBufferSize(swiftConfig, pid, dataDescription,
                    customizeWriterBufferSize))
    {
        maxBufferSize = customizeWriterBufferSize;
        return true;
    }

    if (pid.step() == BUILD_STEP_INC) {
        maxBufferSize = DEFAULT_WRITER_BUFFER_SIZE;
        return true;
    }

    if (swiftConfig.getFullBrokerTopicConfig()->get().topicmode()
        != swift::protocol::TOPIC_MODE_MEMORY_PREFER)
    {
        maxBufferSize = DEFAULT_WRITER_BUFFER_SIZE;
        return true;
    }
        
    // pid.step() == BUILD_STEP_FULL
    ProcessorRuleConfig processorRuleConfig;
    if (!getConfig(resourceReader, ResourceReader::getDataTableRelativePath(dataTableName),
                  "processor_rule_config", processorRuleConfig))
    {
        return false;
    }
    BuildRuleConfig buildRuleConfig;
    if (!getConfig(resourceReader, ResourceReader::getClusterConfRelativePath(clusterName),
                  "cluster_config.builder_rule_config", buildRuleConfig))
    {
        return false;
    }

    BuilderClusterConfig builderClusterConfig;
    if (!builderClusterConfig.init(clusterName, *resourceReader)) {
        return false;
    }

    uint64_t totalBuilderMemory = calculateTotalBuilderMemoryUse(
            buildRuleConfig, builderClusterConfig);

    uint32_t processorPartCount = 1;
    string fullProcessorCountStr = getValueFromKeyValueMap(
        dataDescription.kvMap, CUSTOMIZE_FULL_PROCESSOR_COUNT);
    if (!fullProcessorCountStr.empty()) {
        processorPartCount = StringUtil::fromString<uint32_t>(fullProcessorCountStr);
    } else {
        processorPartCount = processorRuleConfig.partitionCount;
    }

    uint32_t processorParallelNum = 1;
    string fullProcessorParallelStr = getValueFromKeyValueMap(
        dataDescription.kvMap, CUSTOMIZE_FULL_PROCESSOR_COUNT);
    if (fullProcessorParallelStr.empty()) {
        processorParallelNum = processorRuleConfig.parallelNum;
    }
    
    uint32_t processorCount = processorPartCount * processorParallelNum;
    double builderMemToProcessorBufferFactor =
        swiftConfig.getFullBrokerTopicConfig()->builderMemToProcessorBufferFactor;
    maxBufferSize = totalBuilderMemory * builderMemToProcessorBufferFactor / processorCount;
    return true;
}

bool ProcessorConfigurator::getCustomizeWriterBufferSize(
        const SwiftConfig& swiftConfig,
        const PartitionId& pid,
        const DataDescription& ds,
        uint64_t& customizeWriterBufferSize)
{
    SwiftTopicConfigPtr swiftTopicConfig;
    if (pid.step() == BUILD_STEP_FULL) {
        swiftTopicConfig = swiftConfig.getFullBrokerTopicConfig();
    } else {
        swiftTopicConfig = swiftConfig.getIncBrokerTopicConfig();
    }
    string key = pid.buildid().datatable() + "." + ds.at(READ_SRC);
    auto iter = swiftTopicConfig->writerMaxBufferSize.find(key);
    if (iter != swiftTopicConfig->writerMaxBufferSize.end()) {
        customizeWriterBufferSize = iter->second;
        return true;
    }
    string writerConfigStr = swiftConfig.getSwiftWriterConfig(pid.step());

    StringTokenizer st(writerConfigStr, ";",
                       StringTokenizer::TOKEN_IGNORE_EMPTY |
                       StringTokenizer::TOKEN_TRIM);
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        StringTokenizer st2(*it, "=",
                            StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_TRIM);
        if (st2.getNumTokens() != 2) {
            BS_LOG(WARN, "Invalid config item: [%s]", (*it).c_str());
            continue;
        }
        string key = st2[0];
        string valueStr = st2[1];
        if (key == "maxBufferSize") {
            if (!StringUtil::fromString(valueStr, customizeWriterBufferSize)) {
                BS_LOG(ERROR, "convert maxBufferSize[%s] to integer failed", valueStr.c_str());
                return false;
            }
            return true;
        }
    }
    return false;
}

uint64_t ProcessorConfigurator::calculateTotalBuilderMemoryUse(
        const BuildRuleConfig& buildRuleConfig,
        const BuilderClusterConfig& builderClusterConfig)
{
    int64_t buildTotalMemMB =
        builderClusterConfig.indexOptions.GetBuildConfig(false).buildTotalMemory;
    int64_t builderMemory = 0;
    if (builderClusterConfig.builderConfig.sortBuild) {
        builderMemory = builderClusterConfig.builderConfig.calculateSortQueueMemory(buildTotalMemMB);
    } else {
        builderMemory = buildTotalMemMB;
    }
    return builderMemory * buildRuleConfig.partitionCount *
        buildRuleConfig.buildParallelNum * 1024 * 1024;
}

bool ProcessorConfigurator::getAllClusters(
        const ResourceReaderPtr& resourceReader,
        const string& dataTable, vector<string>& clusterNames)
{
    if (!resourceReader) {
        BS_LOG(ERROR, "resource reader is null");
        return false;
    }
    return resourceReader->getAllClusters(dataTable, clusterNames);
}

bool ProcessorConfigurator::getSchemaUpdatableClusters(
        const ResourceReaderPtr& resourceReader,
        const string& dataTable, vector<string>& clusterNames)
{
    SchemaUpdatableClusterConfigVec configVec;
    bool isExist = false;
    if (!getSchemaUpdatableClusterConfigVec(resourceReader, dataTable, configVec, isExist)) {
        return false;
    }

    vector<string> allClusters;
    if (!getAllClusters(resourceReader, dataTable, allClusters))
    {
        return false;
    }

    if (allClusters.size() == 1) {
        // single cluster is schema updatable by default
        clusterNames.push_back(allClusters[0]);
        return true;
    }
    
    for (auto& config: configVec) {
        clusterNames.push_back(config.clusterName);
    }
    sort(clusterNames.begin(), clusterNames.end());
    return true;
}

bool ProcessorConfigurator::getSchemaNotUpdatableClusters(
        const ResourceReaderPtr& resourceReader,
        const string& dataTable, vector<string>& clusterNames)
{
    vector<string> allClusters;
    if (!getAllClusters(resourceReader, dataTable, allClusters))
    {
        return false;
    }

    vector<string> schemaUpdatableClusters;
    if (!getSchemaUpdatableClusters(resourceReader, dataTable, schemaUpdatableClusters))
    {
        return false;
    }
    sort(allClusters.begin(), allClusters.end());
    sort(schemaUpdatableClusters.begin(), schemaUpdatableClusters.end());
    set_difference(allClusters.begin(), allClusters.end(),
                   schemaUpdatableClusters.begin(), schemaUpdatableClusters.end(),
                   inserter(clusterNames, clusterNames.begin()));
    return true;
}

bool ProcessorConfigurator::getSchemaUpdatableClusterConfig(
        const ResourceReaderPtr& resourceReader,
        const string& dataTable,
        const string& clusterName,
        SchemaUpdatableClusterConfig &clusterConf)
{    
    vector<string> allCluster;
    if (!getAllClusters(resourceReader, dataTable, allCluster)) {
        return false;
    }

    if (allCluster.size() == 1) {
        // single cluster enable schema updatable
        ProcessorRuleConfig processorRuleConfig;
        if (!getConfig(resourceReader,
                       ResourceReader::getDataTableRelativePath(dataTable),
                       "processor_rule_config", processorRuleConfig))
        {
            return false;
        }
        clusterConf.partitionCount = processorRuleConfig.partitionCount;
        clusterConf.clusterName = clusterName;
        return true;
    }

    SchemaUpdatableClusterConfigVec configVec;
    bool isExist = false;
    if (!getSchemaUpdatableClusterConfigVec(resourceReader, dataTable, configVec, isExist)) {
        return false;
    }
    for (auto& config: configVec) {
        if (clusterName == config.clusterName) {
            clusterConf = config;
            return true;
        }
    }
    return false;
}

bool ProcessorConfigurator::getSchemaUpdatableClusterConfigVec(
        const ResourceReaderPtr& resourceReader, const string& dataTableName,
        SchemaUpdatableClusterConfigVec &configVec, bool &isConfigExist)
{
    if (!resourceReader) {
        BS_LOG(ERROR, "resource reader is null");
        return false;
    }
    
    if (!resourceReader->getDataTableConfigWithJsonPath(
                    dataTableName, "schema_updatable_clusters",
                    configVec, isConfigExist))
    {
        return false;
    }

    vector<string> clusterNames;
    if (!resourceReader->getAllClusters(dataTableName, clusterNames)) {
        BS_LOG(ERROR, "getAllClusters from dataTable [%s] fail!", dataTableName.c_str());
        return false;
    }
    
    for (auto& config : configVec) {
        if (!config.validate()) {
            return false;
        }

        if (find(clusterNames.begin(), clusterNames.end(),
                 config.clusterName) == clusterNames.end())
        {
            BS_LOG(ERROR, "cluster [%s] in schema_updatable_clusters not exist!",
                   config.clusterName.c_str());
            return false;
        }
    }
    return true;
}

}
}
