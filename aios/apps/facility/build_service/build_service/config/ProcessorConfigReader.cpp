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
#include "build_service/config/ProcessorConfigReader.h"

#include <iosfwd>

#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/SrcNodeConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/RangeUtil.h"

using namespace std;
using build_service::proto::Range;

namespace build_service { namespace config {
BS_LOG_SETUP(config, ProcessorConfigReader);

ProcessorConfigReader::ProcessorConfigReader(const ResourceReaderPtr& resourceReader, const std::string& dataTable,
                                             const std::string& processorConfig)
    : _resourceReader(resourceReader)
    , _dataTable(dataTable)
    , _processorConfig(processorConfig)
{
}

ProcessorConfigReader::~ProcessorConfigReader() {}

bool ProcessorConfigReader::validateConfig(const std::vector<std::string>& clusterNames)
{
    ProcessorRuleConfig processorConfig;
    if (!readRuleConfig(&processorConfig)) {
        return false;
    }
    SrcNodeConfig srcNodeConfig;
    bool isExist = false;
    string jsonPath("src_node_config");
    if (!getProcessorConfig(jsonPath, &srcNodeConfig, &isExist)) {
        return false;
    }
    if (!isExist) {
        return true;
    }
    for (auto& clusterName : clusterNames) {
        if (!checkProcessorCountValid(clusterName, processorConfig.partitionCount)) {
            return false;
        }
    }
    if (!srcNodeConfig.validate()) {
        BS_LOG(ERROR, "srcNode config is invalidate");
        return false;
    }
    return true;
}

bool ProcessorConfigReader::checkProcessorCountValid(const std::string& clusterName, uint32_t processorPartitionCount)
{
    BuildRuleConfig buildRuleConfig;
    if (!_resourceReader->getClusterConfigWithJsonPath(clusterName, "cluster_config.builder_rule_config",
                                                       buildRuleConfig)) {
        BS_LOG(ERROR, "parse cluster_config.builder_rule_config for [%s] failed", clusterName.c_str());
        return false;
    }
    auto partitionCount = buildRuleConfig.partitionCount;
    vector<Range> ranges = util::RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, partitionCount, 1);
    vector<Range> processorRanges = util::RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, processorPartitionCount, 1);
    for (auto range : processorRanges) {
        bool match = false;
        for (auto indexRange : ranges) {
            if (range.from() >= indexRange.from() && range.to() <= indexRange.to()) {
                match = true;
            }
        }
        if (!match) {
            BS_LOG(ERROR, "srcNode processor count [%d] not match index partition count [%d]", processorPartitionCount,
                   partitionCount);
            return false;
        }
    }
    return true;
}

bool ProcessorConfigReader::readRuleConfig(ProcessorRuleConfig* processorConfig)
{
    if (!_processorConfig.empty()) {
        std::string processorFile = PROCESSOR_CONFIG_PATH + "/" + _processorConfig + ".json";
        if (!_resourceReader->getConfigWithJsonPath(processorFile, "processor_rule_config", *processorConfig)) {
            string errorMsg = "Invalid processor_rule_config in "
                              "processor/" +
                              string(_processorConfig) + ".json from [" + _resourceReader->getConfigPath() + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        return processorConfig->validate();
    }
    if (!_resourceReader->getDataTableConfigWithJsonPath(_dataTable, "processor_rule_config", *processorConfig)) {
        string errorMsg = "Invalid processor_rule_config in "
                          "data_tables/" +
                          string(_dataTable) + "_table.json from [" + _resourceReader->getConfigPath() + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return processorConfig->validate();
}

}} // namespace build_service::config
