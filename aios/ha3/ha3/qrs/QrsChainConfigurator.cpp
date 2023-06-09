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
#include "ha3/qrs/QrsChainConfigurator.h"

#include <stddef.h>
#include <iostream>
#include <utility>

#include "alog/Logger.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "fslib/util/FileUtil.h"

#include "ha3/config/ProcessorInfo.h"
#include "ha3/config/QrsChainInfo.h"
#include "ha3/isearch.h"
#include "ha3/qrs/QrsChainConfig.h"
#include "autil/Log.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace fslib::util;
using namespace isearch::config;
namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, QrsChainConfigurator);

QrsChainConfigurator::QrsChainConfigurator() { 
}

QrsChainConfigurator::~QrsChainConfigurator() { 
}

bool QrsChainConfigurator::parseFromFile(const string &configFile, 
                                 QrsChainConfig &qrsConf)
{
    string configFileContent = FileUtil::readFile(configFile);
    return parseFromString(configFileContent, qrsConf);
}

bool QrsChainConfigurator::
parseFromString(const string &configFileContent, QrsChainConfig &qrsConf) {
    try {
        Any any = ParseJson(configFileContent);
        JsonMap jsonConf = AnyCast<JsonMap> (any);

        if (jsonConf.find(PROCESSORS_TAG) != jsonConf.end() 
            && jsonConf.find(CHAINS_TAG) != jsonConf.end())
        {
            JsonArray processors = AnyCast<JsonArray>(jsonConf[PROCESSORS_TAG]);
            map<string, ProcessorInfo> &processorInfoMap = qrsConf.processorInfoMap;
            parseProcessorInfos(processors, processorInfoMap);

            JsonArray chains = AnyCast<JsonArray>(jsonConf[CHAINS_TAG]);
            map<string, QrsChainInfo> &chainInfoMap = qrsConf.chainInfoMap;
            parseChainInfos(chains, chainInfoMap);
        } else {
            AUTIL_LOG(ERROR, "Invalid qrs config: \n%s\n", configFileContent.c_str());
            return false;
        }
    } catch(exception &e) {
        AUTIL_LOG(ERROR, "cached a exception, msg: %s", e.what());
        return false;
    }
    return true;    
}

bool QrsChainConfigurator::parseProcessorInfos(JsonArray &processors, 
        map<string, ProcessorInfo> &processorInfoMap) 
{
    for (size_t i = 0; i < processors.size(); i++)
    {
        JsonMap pInfoJson = AnyCast<JsonMap>(processors[i]);
        string processorName = AnyCast<string>(pInfoJson[PROCESSOR_NAME_TAG]);
        string moduleName = AnyCast<string>(pInfoJson[PROCESSOR_MODULE_TAG]);

        JsonMap jsonParams = AnyCast<JsonMap>(pInfoJson[PROCESSOR_PARAMETER_TAG]);
        KeyValueMap params;
        for (JsonMap::iterator it = jsonParams.begin(); it != jsonParams.end(); ++it)
        {
            string key = it->first;
            string value = AnyCast<string>(it->second);
            params[key] = value;
        }

        ProcessorInfo pInfo(processorName, moduleName);
        pInfo.setParams(params);
        processorInfoMap[processorName] = pInfo;
    }
    return true;
}

bool QrsChainConfigurator::parseChainInfos(JsonArray &chains, 
        map<string, QrsChainInfo> &chainInfoMap) 
{
    for (size_t i = 0; i < chains.size(); i++)
    {
        JsonMap chainInfo = AnyCast<JsonMap>(chains[i]);
        string chainName = AnyCast<string>(chainInfo[CHAIN_NAME_TAG]);
        QrsChainInfo cInfo;
        cInfo.setChainName(chainName);

        JsonArray pluginPoints = AnyCast<JsonArray>(chainInfo[CHAIN_POINTS_TAG]);
        for (size_t j = 0; j < pluginPoints.size(); j++)
        {
            JsonMap point = AnyCast<JsonMap>(pluginPoints[j]);
            string pointName = AnyCast<string>(point[CHAIN_POINT_TAG]);
            JsonArray jsonProcessorNames = AnyCast<JsonArray>(point[CHAIN_PROCESSOR_TAG]);
            ProcessorNameVec processorNames;
            for (size_t k = 0; k < jsonProcessorNames.size(); k++)
            {
                processorNames.push_back(AnyCast<string>(jsonProcessorNames[k]));
            }
            cInfo.addPluginPoint(pointName, processorNames);
        }
        chainInfoMap[chainName] = cInfo;
    }
    return true;
}

} // namespace qrs
} // namespace isearch

