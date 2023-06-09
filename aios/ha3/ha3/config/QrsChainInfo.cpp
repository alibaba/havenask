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
#include "ha3/config/QrsChainInfo.h"

#include <iosfwd>
#include <utility>

#include "alog/Logger.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"

#include "autil/Log.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
namespace isearch {
namespace config {
AUTIL_LOG_SETUP(ha3, QrsChainInfo);

ProcessorNameVec QrsChainInfo::getPluginPoint(const std::string &pointName) const
{
    QrsPluginPoints::const_iterator it = _pluginPoints.find(pointName);
    if (it != _pluginPoints.end()) {
        return it->second;
    } else {
        ProcessorNameVec nullProcessorNameVec;
        return nullProcessorNameVec;
    }
}

void QrsChainInfo::addPluginPoint(const std::string &pointName,
                                  const ProcessorNameVec &pointProcessors)
{
    if (_pluginPoints.find(pointName) == _pluginPoints.end()) {
        _pluginPoints[pointName] = pointProcessors;
    } else {
        AUTIL_LOG(WARN, "Add qrs plugin fail %s", pointName.c_str());
    }
}

void QrsChainInfo::addProcessor(const std::string &pointName,
                                const std::string &processorName)
{
    _pluginPoints[pointName].push_back(processorName);
}

void QrsChainInfo::Jsonize(JsonWrapper& json) {
    json.Jsonize("chain_name", _chainName);
    json.Jsonize("plugin_points", _pluginPoints, QrsPluginPoints());
}
} // namespace config
} // namespace isearch
