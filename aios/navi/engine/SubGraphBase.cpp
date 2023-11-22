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
#include "navi/engine/SubGraphBase.h"
#include "navi/engine/Graph.h"
#include "navi/log/NaviLogger.h"

namespace navi {

void ReplaceInfoMap::init(const SubGraphDef *subGraphDef) {
    auto count = subGraphDef->replace_infos_size();
    std::vector<std::string> resourceList;
    std::unordered_set<std::string> resourceSet;
    for (int i = 0; i < count; i++) {
        const auto &replaceInfo = subGraphDef->replace_infos(i);
        const auto &from = replaceInfo.from();
        if (resourceSet.end() != resourceSet.find(from)) {
            continue;
        }
        resourceSet.insert(from);
        resourceList.push_back(from);
    }
    auto replaceList = resourceList;
    for (int i = 0; i < count; i++) {
        const auto &replaceInfo = subGraphDef->replace_infos(i);
        const auto &from = replaceInfo.from();
        const auto &to = replaceInfo.to();
        for (int j = 0; j < replaceList.size(); j++) {
            auto &tmp = replaceList[j];
            if (tmp == from) {
                tmp = to;
            }
        }
    }
    for (int i = 0; i < resourceList.size(); i++) {
        const auto &from = resourceList[i];
        const auto &to = replaceList[i];
        if (from != to) {
            _replaceFrom2To[from] = to;
        }
    }
    for (const auto &pair : _replaceFrom2To) {
        _replaceTo2From[pair.second].push_back(pair.first);
    }
    NAVI_KERNEL_LOG(DEBUG, "resource replace map: %s", autil::StringUtil::toString(_replaceFrom2To).c_str());
}

const std::string &ReplaceInfoMap::getReplaceRTo(const std::string &from) const {
    auto it = _replaceFrom2To.find(from);
    if (_replaceFrom2To.end() == it) {
        return from;
    }
    return it->second;
}

const std::vector<std::string> *ReplaceInfoMap::getReplaceRFrom(const std::string &to) const {
    auto it = _replaceTo2From.find(to);
    if (_replaceTo2From.end() == it) {
        return nullptr;
    }
    return &it->second;
}

void ReplaceInfoMap::replace(const std::map<std::string, bool> *dependMap, ResourceMap &resourceMap) const {
    if (!dependMap) {
        return;
    }
    if (_replaceFrom2To.empty()) {
        return;
    }
    ResourceMap newMap;
    for (const auto &pair : *dependMap) {
        const auto &name = pair.first;
        const auto &replaceTo = getReplaceRTo(name);
        auto resource = resourceMap.getResource(replaceTo);
        newMap.addResource(name, resource);
    }
    resourceMap.setResource(newMap);
}

SubGraphBase::SubGraphBase(SubGraphDef *subGraphDef,
                           GraphParam *param,
                           const GraphDomainPtr &domain,
                           const SubGraphBorderMapPtr &borderMap,
                           Graph *graph)
    : _logger(this, "SubGraph", domain->getLogger().logger)
    , _subGraphDef(subGraphDef)
    , _param(param)
    , _domain(domain)
    , _borderMap(borderMap)
    , _graph(graph)
    , _terminated(false)
{
    _logger.addPrefix("%d", getGraphId());
}

SubGraphBase::~SubGraphBase() {
    NAVI_LOG(SCHEDULE1, "deleted");
}


GraphId SubGraphBase::getGraphId() const {
    return _subGraphDef->graph_id();
}

NaviPartId SubGraphBase::getPartCount() const {
    return _subGraphDef->location().part_info().part_count();
}

NaviPartId SubGraphBase::getPartId() const {
    return _subGraphDef->location().this_part_id();
}

void SubGraphBase::terminateThis(ErrorCode ec) {
    bool expect = false;
    if (!_terminated.compare_exchange_weak(expect, true)) {
        return;
    }
    terminate(ec);
}

bool SubGraphBase::terminated() const {
    return _terminated.load(std::memory_order_relaxed);
}

const SubGraphDef &SubGraphBase::getSubGraphDef() const {
    return *_subGraphDef;
}

void SubGraphBase::setErrorCode(ErrorCode ec) {
    _param->graphResult->setError(_logger.logger, ec);
}

const std::string &SubGraphBase::getBizName() const {
    return _subGraphDef->location().biz_name();
}

Graph *SubGraphBase::getGraph() const {
    return _graph;
}

GraphParam *SubGraphBase::getParam() const {
    return _param;
}

NaviWorkerBase *SubGraphBase::getWorker() const {
    return _param->worker;
}

}
