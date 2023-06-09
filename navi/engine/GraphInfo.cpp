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
#include "navi/engine/GraphInfo.h"
#include "navi/log/NaviLogger.h"
#include "navi/util/CommonUtil.h"
#include <fslib/fslib.h>

namespace navi {

GraphInfo::GraphInfo(const std::string &configPath, const GraphConfig &config)
    : _configPath(configPath)
    , _config(config)
{
}

GraphInfo::~GraphInfo() {
}

bool GraphInfo::init() {
    if (!loadGraphDef()) {
        return false;
    }
    return true;
}

bool GraphInfo::loadGraphDef() {
    auto absPath = CommonUtil::getConfigAbsPath(_configPath, _config.graphFile);
    std::string fileContent;
    if (fslib::EC_OK !=
        fslib::fs::FileSystem::readFile(absPath, fileContent))
    {
        NAVI_KERNEL_LOG(ERROR, "read file [%s] failed",
                        absPath.c_str());
        return false;
    }
    if (!_graphDef.ParseFromString(fileContent)) {
        NAVI_KERNEL_LOG(ERROR, "parse graph def failed, file [%s], content: %s",
                        absPath.c_str(), fileContent.c_str());
        return false;
    }
    return true;
}

const GraphDef &GraphInfo::getGraphDef() const {
    return _graphDef;
}

}

