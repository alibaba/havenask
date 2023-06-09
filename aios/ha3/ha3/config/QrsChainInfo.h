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
#include <vector>

#include "autil/legacy/jsonizable.h"

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace config {

typedef std::vector<std::string> ProcessorNameVec;
typedef std::map<std::string, ProcessorNameVec> QrsPluginPoints;
class QrsChainInfo : public autil::legacy::Jsonizable
{
public:
    QrsChainInfo() 
        : _chainName("DEFAULT") {}
    QrsChainInfo(const std::string &chainName) 
        : _chainName(chainName) {}
    ~QrsChainInfo() {}
public:
    const std::string& getChainName() const {return _chainName;}
    void setChainName(const std::string& chainName){_chainName = chainName;}
    ProcessorNameVec getPluginPoint(const std::string& pointName) const;
    void addPluginPoint(const std::string &pointName, 
                        const ProcessorNameVec &pointProcessors);

    void addProcessor(const std::string &pointName,
                      const std::string &processorName);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

public:
    std::string _chainName;
    QrsPluginPoints _pluginPoints;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace config
} // namespace isearch

