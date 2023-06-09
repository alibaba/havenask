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
#ifndef NAVI_NAVICONFIG_H
#define NAVI_NAVICONFIG_H

#include "navi/common.h"
#include "navi/config/GraphConfig.h"
#include "navi/config/LogConfig.h"
#include "navi/config/NaviRegistryConfigMap.h"
#include "autil/legacy/jsonizable.h"
#include <map>

namespace navi {

class NaviBizConfig : public autil::legacy::Jsonizable {
public:
    NaviBizConfig();
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
public:
    NaviPartId partCount;
    std::vector<NaviPartId> partIds;
    std::string configPath;
    std::string metaInfo;
    std::set<std::string> initResources;
    std::vector<NaviRegistryConfig> kernelConfigVec;
    std::vector<NaviRegistryConfig> resourceConfigVec;
    std::map<std::string, GraphConfig> graphConfigMap;
};

NAVI_TYPEDEF_PTR(NaviBizConfig);

class ConcurrencyConfig : public autil::legacy::Jsonizable {
public:
    ConcurrencyConfig();
    ConcurrencyConfig(int threadNum_, size_t queueSize_, size_t processingSize_);
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
public:
    int threadNum;
    size_t minThreadNum;
    size_t maxThreadNum;
    size_t queueSize;
    size_t processingSize;
};

class EngineConfig : public autil::legacy::Jsonizable {
public:
    EngineConfig();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    void initExtraQueueFromStr(const std::string &configStr);
public:
    bool disablePerf;
    bool disableSymbolTable;
    ConcurrencyConfig builtinTaskQueue;
    std::map<std::string, ConcurrencyConfig> extraTaskQueueMap;
};

typedef std::map<std::string, NaviBizConfig> NaviBizConfigMap;

class NaviConfig : public autil::legacy::Jsonizable
{
public:
    NaviConfig();
    ~NaviConfig();
private:
    NaviConfig(const NaviConfig &);
    NaviConfig &operator=(const NaviConfig &);
public:
    bool loadConfig(const std::string &configStr);
    bool dumpToStr(std::string &configStr) const;
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
public:
    static bool parseToDocument(std::string jsonStr,
                                autil::legacy::RapidDocument &document);
public:
    autil::legacy::RapidDocument document;
    std::string configStr;
    std::string configPath;
    std::vector<std::string> modules;
    LogConfig logConfig;
    EngineConfig engineConfig;
    NaviBizConfigMap bizMap;
    std::vector<NaviRegistryConfig> buildinResourceConfigVec;
    std::set<std::string> buildinInitResources;
};

NAVI_TYPEDEF_PTR(NaviConfig);

}

#endif //NAVI_NAVICONFIG_H
