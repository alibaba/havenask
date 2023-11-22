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
#include "navi/engine/RpcGraphResult.h"

namespace navi {

NaviErrorPtr SinglePartResult::addResult(
        NaviMessage *message,
        const multi_call::GigStreamRpcInfo *rpcInfo,
        const NaviErrorPtr &error)
{
    autil::ScopedLock lock(_resultMutex);
    if (!message) {
        return doAddResult(nullptr, rpcInfo, error);
    }
    auto resultCount = message->sub_result_size();
    for (int i = 0; i < resultCount; i++) {
        doAddResult(message->mutable_sub_result(i), nullptr, error);
    }
    if (message->has_navi_result()) {
        return doAddResult(message->mutable_navi_result(), rpcInfo, error);
    } else {
        return nullptr;
    }
}

NaviErrorPtr SinglePartResult::doAddResult(
        SerializedResultDef *resultDef,
        const multi_call::GigStreamRpcInfo *rpcInfo,
        const NaviErrorPtr &error)
{
    if (!resultDef && !rpcInfo) {
        return nullptr;
    }
    NaviErrorPtr resultError = error;
    if (resultError) {
        markLocation(&resultError->location);
    }
    auto newDef = std::make_shared<SerializedResultDef>();
    if (resultDef) {
        if(resultDef->has_error()) {
            resultError = std::make_shared<NaviError>();
            resultError->fromProto(resultDef->error());
            markLocation(&resultError->location);
        }
        newDef->Swap(resultDef);
    }
    if (resultError && !newDef->has_error()) {
        resultError->toProto(*newDef->mutable_error());
    }
    NAVI_KERNEL_LOG(SCHEDULE2, "location before mark: [%s]",
                    newDef->location().DebugString().c_str());
    auto locationDef = newDef->mutable_location();
    auto thisIndex = locationDef->locations_size();
    markLocation(newDef->mutable_location());
    if (rpcInfo) {
        auto partLocation = locationDef->mutable_locations(thisIndex)
                                ->mutable_rpc_part_location();
        addRpcInfo(*rpcInfo, partLocation);
    }
    NAVI_KERNEL_LOG(SCHEDULE2, "location after mark: [%s]",
                    newDef->location().DebugString().c_str());
    _results.push_back(newDef);
    return resultError;
}

void SinglePartResult::addRpcInfo(const multi_call::GigStreamRpcInfo &rpcInfo,
                                  RpcPartResultLocationDef *partLocation)
{
    auto rpcInfoDef = partLocation->mutable_rpc_info();
    rpcInfoDef->set_part_id(rpcInfo.partId);
    rpcInfoDef->set_send_count(rpcInfo.sendCount);
    rpcInfoDef->set_receive_count(rpcInfo.receiveCount);
    rpcInfoDef->set_send_begin_ts(rpcInfo.sendBeginTs);
    rpcInfoDef->set_send_end_ts(rpcInfo.sendEndTs);
    rpcInfoDef->set_send_status(rpcInfo.sendStatus);
    rpcInfoDef->set_receive_begin_ts(rpcInfo.receiveBeginTs);
    rpcInfoDef->set_receive_end_ts(rpcInfo.receiveEndTs);
    rpcInfoDef->set_receive_status(rpcInfo.receiveStatus);
    rpcInfoDef->set_spec(rpcInfo.spec);
    rpcInfoDef->set_is_retry(rpcInfo.isRetry);
}

multi_call::GigStreamRpcInfo SinglePartResult::parseSingleGigRpcInfoDef(
        const SingleGigRpcInfoDef &rpcInfoDef)
{
    multi_call::GigStreamRpcInfo rpcInfo;
    rpcInfo.partId = rpcInfoDef.part_id();
    rpcInfo.sendCount = rpcInfoDef.send_count();
    rpcInfo.receiveCount = rpcInfoDef.receive_count();
    rpcInfo.sendBeginTs = rpcInfoDef.send_begin_ts();
    rpcInfo.sendEndTs = rpcInfoDef.send_end_ts();
    rpcInfo.sendStatus =
        (multi_call::GigStreamRpcStatus)rpcInfoDef.send_status();
    rpcInfo.receiveBeginTs = rpcInfoDef.receive_begin_ts();
    rpcInfo.receiveEndTs = rpcInfoDef.receive_end_ts();
    rpcInfo.receiveStatus =
        (multi_call::GigStreamRpcStatus)rpcInfoDef.receive_status();
    rpcInfo.spec = rpcInfoDef.spec();
    rpcInfo.isRetry = rpcInfoDef.is_retry();
    return rpcInfo;
}

void SinglePartResult::doToProto(bool partial, NaviMessage &naviMessage) {
    autil::ScopedLock lock(_resultMutex);
    for (const auto &resultDef : _results) {
        auto newResultDef = naviMessage.add_sub_result();
        newResultDef->Swap(resultDef.get());
    }
    _results.clear();
}

void SinglePartResult::markThisLocation(
    SingleResultLocationDef *singleLocation) const
{
    singleLocation->set_type(NRT_RPC_PART);
    auto rpcPartLocation = singleLocation->mutable_rpc_part_location();
    rpcPartLocation->set_part_id(_partId);
}

void SinglePartResult::doToVisProto(GraphVisNodeDef *visDef) {
    autil::ScopedLock lock(_resultMutex);
    for (const auto &resultPtr : _results) {
        const auto &result = *resultPtr;
        auto thisDef = getVisNode(result.location(), visDef);
        if (!thisDef) {
            NAVI_KERNEL_LOG(ERROR, "result ignored, invalid location");
            continue;
        }
        const auto &resultStr = result.result_str();
        if (!thisDef->has_result()) {
            auto thisResultDef = thisDef->mutable_result();
            if (!thisResultDef->ParseFromString(resultStr)) {
                NAVI_KERNEL_LOG(ERROR, "parse result failed, ignored");
                continue;
            }
            if (thisResultDef->has_symbol_table()) {
                auto symbolTableDef = thisResultDef->mutable_symbol_table();
                visDef->mutable_symbol_table()->MergeFrom(*symbolTableDef);
                thisResultDef->clear_symbol_table();
            }
        } else {
            GraphResultDef resultDef;
            if (!resultDef.ParseFromString(resultStr)) {
                NAVI_KERNEL_LOG(ERROR, "parse result failed, ignored");
                continue;
            }
            auto thisResultDef = thisDef->mutable_result();
            if (resultDef.has_session()) {
                thisResultDef->mutable_session()->MergeFrom(resultDef.session());
            }
            if (resultDef.has_error()) {
                thisResultDef->mutable_error()->MergeFrom(resultDef.error());
            }
            if (resultDef.has_graph()) {
                thisResultDef->mutable_graph()->MergeFrom(resultDef.graph());
            }
            if (resultDef.has_traces()) {
                thisResultDef->mutable_traces()->MergeFrom(resultDef.traces());
            }
            if (resultDef.has_graph_metric()) {
                thisResultDef->mutable_graph_metric()->MergeFrom(
                    resultDef.graph_metric());
            }
            if (resultDef.has_symbol_table()) {
                visDef->mutable_symbol_table()->MergeFrom(
                    resultDef.symbol_table());
            }
            if (resultDef.has_host_info()) {
                thisResultDef->mutable_host_info()->MergeFrom(
                    resultDef.host_info());
            }
        }
    }
}

void SinglePartResult::collectPartRpcInfo(
    const RpcResultLocationDef &rpcLoc,
    const RpcPartResultLocationDef &partLocation,
    multi_call::GigStreamRpcInfoMap &rpcInfoMap)
{
    if (!partLocation.has_rpc_info()) {
        return;
    }
    char buf[256];
    snprintf(buf, 256, "%s(%d)", rpcLoc.from_biz().c_str(),
             rpcLoc.from_part_id());
    auto key = std::pair(buf, rpcLoc.to_biz());
    const auto &info = parseSingleGigRpcInfoDef(partLocation.rpc_info());
    auto it = rpcInfoMap.find(key);
    if (it != rpcInfoMap.end()) {
        it->second.emplace_back(std::move(info));
    } else {
        rpcInfoMap[key] = {std::move(info)};
    }
}

void SinglePartResult::doCollectRpcInfo(
    multi_call::GigStreamRpcInfoMap &rpcInfoMap)
{
    autil::ScopedLock lock(_resultMutex);
    for (const auto &resultPtr : _results) {
        const auto &resultLocation = resultPtr->location();
        auto count = resultLocation.locations_size();
        if (count <= 0) {
            continue;
        }
        const RpcResultLocationDef *rpcLoc = nullptr;
        for (int i = count - 1; i >= 0; i--) {
            const auto &singleLoc = resultLocation.locations(i);
            auto type = singleLoc.type();
            if (NRT_RPC == type) {
                rpcLoc = &singleLoc.rpc_location();
                continue;
            }
            if (NRT_RPC_PART == type && rpcLoc) {
                collectPartRpcInfo(*rpcLoc, singleLoc.rpc_part_location(),
                                   rpcInfoMap);
            }
            rpcLoc = nullptr;
        }
    }
}

void SinglePartResult::doCollectTrace(TraceCollector &collector,
                                      SymbolTableDef &symbolTableDef)
{
    autil::ScopedLock lock(_resultMutex);
    std::vector<std::shared_ptr<GraphResultDef>> resultDefVec;
    for (const auto &resultPtr : _results) {
        if (!resultPtr->has_trace()) {
            continue;
        }
        auto resultDef = std::make_shared<GraphResultDef>();
        if (!resultDef->ParseFromString(resultPtr->result_str())) {
            NAVI_KERNEL_LOG(ERROR, "parse result failed, ignored");
            continue;
        }
        if (resultDef->has_symbol_table()) {
            symbolTableDef.MergeFrom(resultDef->symbol_table());
        }
        resultDefVec.push_back(resultDef);
    }
    for (const auto &resultDefPtr : resultDefVec) {
        if (!resultDefPtr->has_traces()) {
            continue;
        }
        TraceCollector thisCollector;
        thisCollector.fromProto(resultDefPtr->traces(), symbolTableDef);
        collector.merge(thisCollector);
    }
}

void RpcGraphResult::init(const std::string &from, NaviPartId fromPartId,
                          const std::string &to, const PartInfo &partInfo,
                          GraphId graphId)
{
    _fromBiz = from;
    _fromPartId = fromPartId;
    _toBiz = to;
    _partCount = partInfo.getFullPartCount();
    _graphId = graphId;
    for (NaviPartId partId = 0; partId < _partCount; partId++) {
        if (!partInfo.isUsed(partId)) {
            continue;
        }
        addSubResult(partId, std::make_shared<SinglePartResult>(partId));
    }
}

NaviErrorPtr RpcGraphResult::addResult(NaviPartId partId, NaviMessage *message,
                                       const multi_call::GigStreamRpcInfo *rpcInfo,
                                       const NaviErrorPtr &error)
{
    auto subResult = getSubResult(partId);
    if (!subResult) {
        return nullptr;
    }
    auto singleResult = static_cast<SinglePartResult *>(subResult);
    return singleResult->addResult(message, rpcInfo, error);
}

void RpcGraphResult::markThisLocation(
    SingleResultLocationDef *singleLocation) const
{
    singleLocation->set_type(NRT_RPC);
    auto rpcLocation = singleLocation->mutable_rpc_location();
    rpcLocation->set_from_biz(_fromBiz);
    rpcLocation->set_from_part_id(_fromPartId);
    rpcLocation->set_to_biz(_toBiz);
    rpcLocation->set_part_count(_partCount);
    rpcLocation->set_graph_id(_graphId);
}

}

