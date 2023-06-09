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

#include <map>
#include <string>

#include "autil/legacy/json.h"

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace config {
class ProcessorInfo;
class QrsChainInfo;
}  // namespace config
namespace qrs {
struct QrsChainConfig;
}  // namespace qrs
}  // namespace isearch

namespace isearch {
namespace qrs {

//tag in config file
#define PROCESSORS_TAG "processors"
#define PROCESSOR_NAME_TAG "processor_name"
#define PROCESSOR_MODULE_TAG "module_name"
#define PROCESSOR_PARAMETER_TAG "parameters"

#define CHAINS_TAG "chains"
#define CHAIN_NAME_TAG "chain_name"
#define CHAIN_POINTS_TAG "plugin_points"
#define CHAIN_POINT_TAG "plugin_point"
#define CHAIN_PROCESSOR_TAG "processors"


//allowed plugin point
#define BEFORE_PARSER_POINT "BEFORE_PARSER_POINT"
#define BEFORE_VALIDATE_POINT "BEFORE_VALIDATE_POINT"
#define BEFORE_SEARCH_POINT "BEFORE_SEARCH_POINT"


/** this class is to parse the 'QrsChainConfig' object from config file.*/
class QrsChainConfigurator
{
public:
    QrsChainConfigurator();
    ~QrsChainConfigurator();
public:
    bool parseFromFile(const std::string &configFile,
                       QrsChainConfig &qrsConf);
    bool parseFromString(const std::string &configFileContent,
                         QrsChainConfig &qrsConf);
private:
    bool parseProcessorInfos(autil::legacy::json::JsonArray &precessors,
                             std::map<std::string, config::ProcessorInfo> &processorInfoMap);
    bool parseChainInfos(autil::legacy::json::JsonArray &chains,
                         std::map<std::string, config::QrsChainInfo> &chainInfoMap);
private:
    AUTIL_LOG_DECLARE();
};

} // namespace qrs
} // namespace isearch
