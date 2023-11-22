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

#include "navi/proto/GraphVis.pb.h"
#include "navi/engine/GraphResultBase.h"
#include "navi/engine/PartInfo.h"
#include "navi/engine/NaviError.h"
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"

namespace navi {

class SinglePartResult : public GraphResultBase {
public:
    SinglePartResult(NaviPartId partId)
        : _partId(partId)
    {
    }
public:
    NaviErrorPtr addResult(NaviMessage *message,
                           const multi_call::GigStreamRpcInfo *rpcInfo,
                           const NaviErrorPtr &error);
private:
    void doToProto(bool partial, NaviMessage &naviMessage) override;
    void markThisLocation(SingleResultLocationDef *singleLocation) const override;
    void doToVisProto(GraphVisNodeDef *visDef) override;
    void doCollectRpcInfo(multi_call::GigStreamRpcInfoMap &rpcInfoMap) override;
    void doCollectTrace(TraceCollector &collector,
                        SymbolTableDef &symbolTableDef) override;
private:
    NaviErrorPtr doAddResult(SerializedResultDef *resultDef,
                             const multi_call::GigStreamRpcInfo *rpcInfo,
                             const NaviErrorPtr &error);
private:
    static void addRpcInfo(const multi_call::GigStreamRpcInfo &rpcInfo,
                           RpcPartResultLocationDef *partLocation);
    static void collectPartRpcInfo(const RpcResultLocationDef &rpcLoc,
                                   const RpcPartResultLocationDef &partLocation,
                                   multi_call::GigStreamRpcInfoMap &rpcInfoMap);
    static multi_call::GigStreamRpcInfo parseSingleGigRpcInfoDef(
            const SingleGigRpcInfoDef &rpcInfoDef);
private:
    NaviPartId _partId = INVALID_NAVI_PART_ID;
    mutable autil::ThreadMutex _resultMutex;
    std::vector<std::shared_ptr<SerializedResultDef>> _results;
};

NAVI_TYPEDEF_PTR(SinglePartResult);

class RpcGraphResult : public GraphResultBase
{
public:
    void init(const std::string &from, NaviPartId fromPartId,
              const std::string &to, const PartInfo &partInfo,
              GraphId graphId);
    NaviErrorPtr addResult(NaviPartId partId, NaviMessage *message,
                           const multi_call::GigStreamRpcInfo *rpcInfo,
                           const NaviErrorPtr &error);
private:
    void markThisLocation(SingleResultLocationDef *singleLocation) const override;
private:
    std::string _fromBiz;
    NaviPartId _fromPartId = INVALID_NAVI_PART_ID;
    std::string _toBiz;
    NaviPartId _partCount = INVALID_NAVI_PART_COUNT;
    GraphId _graphId = INVALID_GRAPH_ID;
};

NAVI_TYPEDEF_PTR(RpcGraphResult);

}

