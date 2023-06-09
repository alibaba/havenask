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
#ifndef ISEARCH_BS_PROCESSORCONFIGREADER_H
#define ISEARCH_BS_PROCESSORCONFIGREADER_H

#include "build_service/common_define.h"
#include "build_service/config/ProcessorRuleConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/SrcNodeConfig.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class ProcessorConfigReader
{
public:
    ProcessorConfigReader(const ResourceReaderPtr& resourceReader, const std::string& dataTable,
                          const std::string& processorConfig);
    ~ProcessorConfigReader();

private:
    ProcessorConfigReader(const ProcessorConfigReader&);
    ProcessorConfigReader& operator=(const ProcessorConfigReader&);

public:
    bool readRuleConfig(ProcessorRuleConfig* processorConfig);
    template <typename T>
    bool getProcessorConfig(const std::string& path, T* processorConfig, bool* isExist);
    template <typename T>
    bool getProcessorConfig(const std::string& path, T* processorConfig);
    ResourceReaderPtr getResourceReader() { return _resourceReader; }
    bool validateConfig(const std::vector<std::string>& clusterNames);

private:
    bool checkProcessorCountValid(const std::string& clusterName, uint32_t processorPartitionCount);

private:
    ResourceReaderPtr _resourceReader;
    std::string _dataTable;
    std::string _processorConfig;

private:
    BS_LOG_DECLARE();
};

template <typename T>
inline bool ProcessorConfigReader::getProcessorConfig(const std::string& path, T* processorConfig)
{
    bool exist;
    return getProcessorConfig(path, processorConfig, &exist);
}

template <typename T>
inline bool ProcessorConfigReader::getProcessorConfig(const std::string& path, T* processorConfig, bool* isExist)
{
    if (!_processorConfig.empty()) {
        std::string processorFile = PROCESSOR_CONFIG_PATH + "/" + _processorConfig + ".json";
        if (!_resourceReader->getConfigWithJsonPath(processorFile, path, *processorConfig, *isExist)) {
            std::string errorMsg = "Invalid " + path +
                                   " in "
                                   "processor/" +
                                   std::string(_processorConfig) + ".json from [" + _resourceReader->getConfigPath() +
                                   "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        return true;
    }
    if (!_resourceReader->getDataTableConfigWithJsonPath(_dataTable, path, *processorConfig, *isExist)) {
        std::string errorMsg = "Invalid " + path +
                               " in "
                               "data_tables/" +
                               std::string(_dataTable) + "_table.json from [" + _resourceReader->getConfigPath() + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

BS_TYPEDEF_PTR(ProcessorConfigReader);

}} // namespace build_service::config

#endif // ISEARCH_BS_PROCESSORCONFIGREADER_H
