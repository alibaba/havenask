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
#include "build_service/admin/controlflow/OpenApiHandler.h"

#include <iosfwd>
#include <memory>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "build_service/common/ResourceKeeper.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, OpenApiHandler);

OpenApiHandler::OpenApiHandler(const TaskResourceManagerPtr& taskResMgr) : _taskResourceManager(taskResMgr) {}

OpenApiHandler::~OpenApiHandler() {}

bool OpenApiHandler::handle(const std::string& cmd, const std::string& path, const KeyValueMap& params)
{
    CmdPathParser parser;
    parser.parse(path);
    if (cmd == "book") {
        if (parser.getType() == CmdPathParser::RESOURCE) {
            return _taskResourceManager->declareResourceKeeper(parser.getResourceType(), params);
        }
    }
    if (cmd == "add") {
        if (parser.getType() == CmdPathParser::RESOURCE) {
            auto resourceKeeper = _taskResourceManager->getResourceKeeper(params);
            if (resourceKeeper) {
                return resourceKeeper->addResource(params);
            } else {
                return false;
            }
        }
    }
    if (cmd == "del") {
        if (parser.getType() == CmdPathParser::RESOURCE) {
            auto resourceKeeper = _taskResourceManager->getResourceKeeper(params);
            if (resourceKeeper) {
                return resourceKeeper->deleteResource(params);
            } else {
                return false;
            }
        }
    }

    if (cmd == "update") {
        if (parser.getType() == CmdPathParser::RESOURCE) {
            auto resourceKeeper = _taskResourceManager->getResourceKeeper(params);
            if (resourceKeeper) {
                return resourceKeeper->updateResource(params);
            } else {
                return false;
            }
        }
    }
    return false;
}

void OpenApiHandler::CmdPathParser::parse(const string& path)
{
    vector<string> paths;
    StringUtil::split(paths, path, '/', /*ignore empty*/ true);
    if (paths.size() < 2) {
        _type = UNKNOWN;
        return;
    }
    if (paths[0] != "resource") {
        _type = UNKNOWN;
        return;
    }
    if (paths.size() == 2) {
        _type = RESOURCE;
        if (paths[1] == "swift") {
            _resourceType = "swift";
        } else if (paths[1] == "index_checkpoint") {
            _resourceType = "index_checkpoint";
        } else if (paths[1] == "checkpoint") {
            _resourceType = "checkpoint";
        }
        return;
    }
    _type = UNKNOWN;
    return;
}

}} // namespace build_service::admin
