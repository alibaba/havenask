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

#include "navi/proto/NaviStream.pb.h"
#include "navi/log/NaviLogger.h"
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"

namespace navi {

class GraphResultBase;
NAVI_TYPEDEF_PTR(GraphResultBase);
class TraceCollector;

class GraphResultBase
{
public:
    GraphResultBase() {
    }
    virtual ~GraphResultBase() {
    }
    GraphResultBase(const GraphResultBase &) = delete;
    GraphResultBase &operator=(const GraphResultBase &) = delete;
public:
    void setParent(GraphResultBase *parent) {
        autil::ScopedLock lock(_mutex);
        _parent = parent;
    }
    bool hasParent() const {
        return (nullptr != _parent);
    }
    void addSubResult(const GraphResultBasePtr &subResult) {
        if (!subResult) {
            return;
        }
        subResult->setParent(this);
        autil::ScopedLock lock(_mutex);
        _subResultVec.push_back(subResult);
    }
    void addSubResult(NaviPartId partId, const GraphResultBasePtr &subResult) {
        if (!subResult) {
            return;
        }
        subResult->setParent(this);
        autil::ScopedLock lock(_mutex);
        _partSubResultMap.emplace(partId, subResult);
    }
    void markLocation(ResultLocationDef *resultLocationDef) const {
        markThisLocation(resultLocationDef->add_locations());
        if (_parent) {
            _parent->markLocation(resultLocationDef);
        }
    }
    virtual void markThisLocation(SingleResultLocationDef *singleLocation) const {
    }
    GraphResultBase *getSubResult(NaviPartId partId) const {
        autil::ScopedLock lock(_mutex);
        auto it = _partSubResultMap.find(partId);
        if (_partSubResultMap.end() == it) {
            return nullptr;
        }
        return it->second.get();
    }
    void toProto(bool partial, NaviMessage &naviMessage) {
        autil::ScopedLock lock(_mutex);
        if (!partial && !_finished) {
            return;
        }
        doToProto(partial, naviMessage);
        for (const auto &result : _subResultVec) {
            result->toProto(partial, naviMessage);
        }
        for (const auto &pair : _partSubResultMap) {
            pair.second->toProto(partial, naviMessage);
        }
    }
    virtual void doToProto(bool partial, NaviMessage &naviMessage) {
    }
    void setFinish() {
        autil::ScopedLock lock(_mutex);
        if (_finished) {
            return;
        }
        _finished = true;
        for (const auto &result : _subResultVec) {
            result->setFinish();
        }
        for (const auto &pair : _partSubResultMap) {
            pair.second->setFinish();
        }
    }
    bool isFinished() const {
        autil::ScopedLock lock(_mutex);
        return _finished;
    }
    void toVisProto(GraphVisNodeDef *parentDef) {
        if (!parentDef) {
            return;
        }
        autil::ScopedLock lock(_mutex);
        doToVisProto(parentDef);
        for (const auto &result : _subResultVec) {
            result->toVisProto(parentDef);
        }
        for (const auto &pair : _partSubResultMap) {
            pair.second->toVisProto(parentDef);
        }
    }
    virtual void doToVisProto(GraphVisNodeDef *visDef) {
    }
    void collectError() {
    }
    virtual void doCollectError() {
    }
    void collectRpcInfo(multi_call::GigStreamRpcInfoMap &rpcInfoMap) {
        autil::ScopedLock lock(_mutex);
        doCollectRpcInfo(rpcInfoMap);
        for (const auto &result : _subResultVec) {
            result->collectRpcInfo(rpcInfoMap);
        }
        for (const auto &pair : _partSubResultMap) {
            pair.second->collectRpcInfo(rpcInfoMap);
        }
    }
    virtual void doCollectRpcInfo(multi_call::GigStreamRpcInfoMap &rpcInfoMap) {
    }
    void collectTrace(TraceCollector &collector, SymbolTableDef &symbolTableDef) {
        autil::ScopedLock lock(_mutex);
        doCollectTrace(collector, symbolTableDef);
        for (const auto &result : _subResultVec) {
            result->collectTrace(collector, symbolTableDef);
        }
        for (const auto &pair : _partSubResultMap) {
            pair.second->collectTrace(collector, symbolTableDef);
        }
    }
    virtual void doCollectTrace(TraceCollector &collector,
                                SymbolTableDef &symbolTableDef)
    {
    }
protected:
    static GraphVisNodeDef *getVisNode(const ResultLocationDef &location,
                                       GraphVisNodeDef *visDef)
    {
        NAVI_KERNEL_LOG(SCHEDULE2, "location: %s",
                        location.DebugString().c_str());
        auto singleLocationCount = location.locations_size();
        if (0 == singleLocationCount) {
            return nullptr;
        }
        auto currentDef = visDef;
        for (int i = singleLocationCount - 1; i >= 0; i--) {
            const auto &singleLoc = location.locations(i);
            if (NRT_NORMAL == singleLoc.type()) {
                if (!currentDef->has_location()) {
                    currentDef->mutable_location()->CopyFrom(singleLoc);
                }
                NAVI_KERNEL_LOG(SCHEDULE2, "normal location");
                continue;
            }
            auto locationKey = getLocationKey(singleLoc);
            NAVI_KERNEL_LOG(SCHEDULE2, "enter location key [%s]",
                            locationKey.c_str());
            auto resultMap = currentDef->mutable_sub_results();
            auto it = resultMap->find(locationKey);
            if (resultMap->end() != it) {
                currentDef = &it->second;
                currentDef->mutable_location()->MergeFrom(singleLoc);
            } else {
                currentDef = &(*resultMap)[locationKey];
                currentDef->mutable_location()->CopyFrom(singleLoc);
            }
        }
        return currentDef;
    }
    static std::string getLocationKey(const SingleResultLocationDef &singleLocation) {
        switch (singleLocation.type()) {
        case NRT_NORMAL:
            return std::string();
        case NRT_FORK: {
            const auto &location = singleLocation.fork_location();
            return "f_" + std::to_string(location.graph_id()) + "_" +
                   location.biz_name() + "_" + location.node_name();
        }
        case NRT_RPC: {
            const auto &location = singleLocation.rpc_location();
            return "r_" + location.from_biz() + "_" +
                   std::to_string(location.from_part_id()) + "_" +
                   location.to_biz() + "_" +
                   std::to_string(location.part_count()) + "_" +
                   std::to_string(location.graph_id());
        }
        case NRT_RPC_PART: {
            const auto &location = singleLocation.rpc_part_location();
            return "p_" + std::to_string(location.part_id());
        }
        default:
            return std::string("unknown");
        }
    }
private:
    mutable autil::ThreadMutex _mutex;
    bool _finished = false;
    GraphResultBase *_parent = nullptr;
    std::vector<GraphResultBasePtr> _subResultVec;
    std::map<NaviPartId, GraphResultBasePtr> _partSubResultMap;
};

}

