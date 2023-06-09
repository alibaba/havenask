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
#include "ha3/turing/qrs/Ha3QrsRequestGenerator.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/CompressionUtil.h"
#include "autil/Log.h"
#include "autil/RangeUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "google/protobuf/arena.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/common/ClusterClause.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/DocIdClause.h"
#include "ha3/common/Request.h"
#include "ha3/config/QrsConfig.h"
#include "ha3/isearch.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/service/QrsRequest.h"
#include "ha3/service/QrsRequestLocal.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/GrpcRequest.h"
#include "aios/network/gig/multi_call/interface/Request.h"
#include "aios/network/gig/multi_call/interface/RequestGenerator.h"
#include "suez/turing/proto/Search.pb.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/protobuf/config.pb.h"

using namespace std;
using namespace tensorflow;
using namespace multi_call;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace isearch::common;
using namespace isearch::proto;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, Ha3QrsRequestGenerator);

std::string Ha3QrsRequestGenerator::SEARCH_METHOD_NAME = "runGraph";

Ha3QrsRequestGenerator::Ha3QrsRequestGenerator(
    const std::string &bizName,
    multi_call::SourceIdTy sourceId,
    multi_call::VersionTy version,
    GenerateType type,
    bool useGrpc,
    common::RequestPtr requestPtr,
    int32_t timeout,
    common::DocIdClause *docIdClause,
    const config::SearchTaskQueueRule &searchTaskqueueRule,
    autil::mem_pool::Pool *pool,
    autil::CompressType compressType,
    const std::shared_ptr<google::protobuf::Arena> &arena,
    suez::RpcServer *rpcServer)
    : RequestGenerator(bizName, arena, sourceId, version)
    , _generateType(type)
    , _useGrpc(useGrpc)
    , _requestPtr(requestPtr)
    , _timeout(timeout)
    , _docIdClause(docIdClause)
    , _searchTaskqueueRule(searchTaskqueueRule)
    , _pool(pool)
    , _compressType(compressType)
    , _rpcServer(rpcServer) {
    assert(arena);
}
Ha3QrsRequestGenerator::~Ha3QrsRequestGenerator() {
    DELETE_AND_SET_NULL(_docIdClause);
}

void Ha3QrsRequestGenerator::generate(PartIdTy partCnt, PartRequestMap &requestMap) {
    switch (_generateType) {
    case GT_NORMAL:
        generateNormal(partCnt, requestMap);
        break;
    case GT_PID:
        generatePid(partCnt, requestMap);
        break;
    case GT_SUMMARY:
        generateSummary(partCnt, requestMap);
        break;
    default:
        AUTIL_LOG(ERROR, "unknow generate type [%d]", _generateType);
    }
}

void Ha3QrsRequestGenerator::prepareRequest(const std::string &bizName,
                                            suez::turing::GraphRequest &graphRequest,
                                            bool needClone) const {
    graphRequest.set_src(_requestPtr->getConfigClause()->getMetricsSrc());
    graphRequest.set_bizname(bizName);
    suez::turing::GraphInfo &graphInfo = *graphRequest.mutable_graphinfo();

    suez::turing::NamedTensorProto &inputRequest = *graphInfo.add_inputs();
    auto inputRequestTensor = Tensor(DT_VARIANT, TensorShape({}));
    inputRequestTensor.scalar<Variant>()() = Ha3RequestVariant(_requestPtr, _pool);
    inputRequest.set_name("ha3_request");
    inputRequestTensor.AsProtoField(inputRequest.mutable_tensor());
    *graphInfo.add_fetches() = HA3_RESULT_TENSOR_NAME;
    *graphInfo.add_targets() = "ha3_search_done";
    graphRequest.set_taskqueuename(getTaskQueueName());
    makeDebugRunOptions(graphRequest);
}

void Ha3QrsRequestGenerator::generateNormal(PartIdTy partCnt, PartRequestMap &requestMap) const {
    if (partCnt <= 0) {
        return;
    }
    auto clusterClause = getClusterClause();
    if (clusterClause) {
        clusterClause->setTotalSearchPartCnt(partCnt);
    }
    auto arena = getProtobufArena().get();
    auto graphRequest0 = google::protobuf::Arena::CreateMessage<suez::turing::GraphRequest>(arena);
    const auto &bizName = getBizName();
    prepareRequest(bizName, *graphRequest0, false);
    graphRequest0->set_partid(0);
    AUTIL_LOG(DEBUG, "generate normal request for partid [%d]", 0);
    graphRequest0->set_totalpartcount(partCnt);
    requestMap[0] = createQrsRequest(SEARCH_METHOD_NAME, graphRequest0);
    for (int32_t i = 1; i < partCnt; i++) {
        auto message = (suez::turing::GraphRequest *)graphRequest0->New(arena);
        assert(message);
        message->CopyFrom(*graphRequest0);
        message->set_partid(i);
        AUTIL_LOG(DEBUG, "generate normal request for partid [%d]", i);
        message->set_totalpartcount(partCnt);
        requestMap[i] = createQrsRequest(SEARCH_METHOD_NAME, message);
    }
}

void Ha3QrsRequestGenerator::generatePid(PartIdTy partCnt, PartRequestMap &requestMap) const {
    if (partCnt <= 0) {
        return;
    }
    auto clusterClause = getClusterClause();
    const auto &bizName = getBizName();
    assert(clusterClause);
    auto &partIdsMap = clusterClause->getClusterPartIdPairsMap();
    auto iterMap = partIdsMap.find(bizName);
    if (iterMap == partIdsMap.end()) {
        return;
    }
    const auto &ranges = RangeUtil::splitRange(0, MAX_PARTITION_RANGE, partCnt);
    const auto &hashIds = iterMap->second;
    set<int32_t> pids;
    if (hashIds.empty()) {
        for (int32_t i = 0; i < partCnt; i++) {
            pids.insert(i);
        }
    } else {
        pids = getPartIds(ranges, hashIds);
    }
    if (pids.empty()) {
        return;
    }
    clusterClause->setTotalSearchPartCnt(pids.size());
    auto arena = getProtobufArena().get();
    auto graphRequestBegin
        = google::protobuf::Arena::CreateMessage<suez::turing::GraphRequest>(arena);
    auto iter = pids.begin();
    prepareRequest(bizName, *graphRequestBegin, false);
    graphRequestBegin->set_partid(*iter);
    graphRequestBegin->set_totalpartcount(partCnt);
    AUTIL_LOG(DEBUG, "generate pid request for partid [%d]", *iter);
    requestMap[*iter] = createQrsRequest(SEARCH_METHOD_NAME, graphRequestBegin);
    for (iter++; iter != pids.end(); iter++) {
        auto index = *iter;
        auto message = (suez::turing::GraphRequest *)graphRequestBegin->New(arena);
        assert(message);
        message->CopyFrom(*graphRequestBegin);
        message->set_partid(index);
        AUTIL_LOG(DEBUG, "generate pid request for partid [%d]", *iter);
        message->set_totalpartcount(partCnt);
        requestMap[index] = createQrsRequest(SEARCH_METHOD_NAME, message);
    }
}

set<int32_t> Ha3QrsRequestGenerator::getPartIds(const RangeVec &ranges, const RangeVec &hashIds) {
    set<int32_t> partIds;
    for (size_t i = 0; i < hashIds.size(); ++i) {
        getPartId(ranges, hashIds[i], partIds);
    }
    return partIds;
}

void Ha3QrsRequestGenerator::getPartId(const RangeVec &ranges,
                                       const PartitionRange &hashId,
                                       set<int32_t> &partIds) {
    for (size_t i = 0; i < ranges.size(); ++i) {
        if (!(ranges[i].second < hashId.first || ranges[i].first > hashId.second)) {
            partIds.insert(i);
        }
    }
}

void Ha3QrsRequestGenerator::generateSummary(PartIdTy partCnt, PartRequestMap &requestMap) const {
    if (partCnt <= 0) {
        return;
    }
    const auto &bizName = getBizName();
    auto request = getRequest();
    auto docIdClause = getDocIdClause();
    request->setDocIdClause(docIdClause);
    auto globalIdVector = docIdClause->getGlobalIdVector();
    const RangeVec &ranges = RangeUtil::splitRange(0, MAX_PARTITION_RANGE, partCnt);
    vector<pair<PartitionRange, GlobalIdVector>> partGidVectors;
    auto arena = getProtobufArena().get();
    Range &tmpRange = *google::protobuf::Arena::CreateMessage<Range>(arena);
    for (uint32_t i = 0; i < ranges.size(); ++i) {
        GlobalIdVector matchedGids;
        tmpRange.set_from(ranges[i].first);
        tmpRange.set_to(ranges[i].second);
        docIdClause->getGlobalIdVector(tmpRange, matchedGids);
        partGidVectors.push_back(make_pair(ranges[i], matchedGids));
    }
    for (uint32_t i = 0; i < partGidVectors.size(); ++i) {
        const auto &rangeGid = partGidVectors[i];
        if (0 == rangeGid.second.size()) {
            continue;
        }
        docIdClause->setGlobalIdVector(rangeGid.second);
        auto graphRequest
            = google::protobuf::Arena::CreateMessage<suez::turing::GraphRequest>(arena);
        prepareRequest(bizName, *graphRequest, true);
        graphRequest->set_partid(i);
        AUTIL_LOG(DEBUG, "generate summary request for partid [%d]", i);
        graphRequest->set_totalpartcount(partCnt);
        requestMap[i] = createQrsRequest(SEARCH_METHOD_NAME, graphRequest);
    }
    docIdClause->setGlobalIdVector(globalIdVector);
    request->stealDocIdClause();
}

multi_call::RequestPtr
Ha3QrsRequestGenerator::createQrsRequest(const string &methodName,
                                         suez::turing::GraphRequest *graphRequest) const {
    multi_call::Request *qrsRequest = NULL;
    if (_useGrpc) {
        auto requestTyped
            = new GrpcRequest("/suez.turing.GraphService/runGraph", getProtobufArena());
        requestTyped->setMessage(graphRequest);
        requestTyped->setTimeout(_timeout);
        qrsRequest = requestTyped;
    } else if (_rpcServer != nullptr) { //直接调用searcher的接口（不进行rpc调用）
        qrsRequest = new service::QrsRequestLocal(
            methodName, graphRequest, _timeout, getProtobufArena(), _rpcServer);
    } else {
        qrsRequest
            = new service::QrsRequest(methodName, graphRequest, _timeout, getProtobufArena());
    }
    return multi_call::RequestPtr(qrsRequest);
}

void Ha3QrsRequestGenerator::makeDebugRunOptions(suez::turing::GraphRequest &graphRequest) const {
    auto configClause = _requestPtr->getConfigClause();
    if (configClause) {
        auto opDebugStr = configClause->getKVPairValue(OP_DEBUG_KEY);
        if (opDebugStr != HA3_EMPTY_STRING) {
            tensorflow::RunOptions *runOptions = graphRequest.mutable_runoptions();
            JsonArray debugWatchOps;
            const vector<string> &debugInfos = autil::StringUtil::split(opDebugStr, ",");
            for (size_t i = 0; i < debugInfos.size(); i++) {
                JsonMap debugWatchOp;
                FromJsonString(debugWatchOp, "{\"nodeName\":\"" + debugInfos[i] + "\"}");
                debugWatchOps.push_back(debugWatchOp);
            }
            JsonMap runOptionsJson, debugTensorWatchOpts;
            debugTensorWatchOpts["debugTensorWatchOpts"] = debugWatchOps;
            runOptionsJson["debugOptions"] = debugTensorWatchOpts;
            http_arpc::ProtoJsonizer::fromJsonMap(runOptionsJson, runOptions);
        }
    }
}

string Ha3QrsRequestGenerator::getTaskQueueName() const {
    auto configClause = _requestPtr->getConfigClause();
    assert(configClause);
    auto phaseNumber = configClause->getPhaseNumber();
    if (SEARCH_PHASE_ONE == phaseNumber) {
        if (!configClause->getPhaseOneTaskQueueName().empty()) {
            return configClause->getPhaseOneTaskQueueName();
        }
        return _searchTaskqueueRule.phaseOneTaskQueue;
    } else {
        if (!configClause->getPhaseTwoTaskQueueName().empty()) {
            return configClause->getPhaseTwoTaskQueueName();
        }
        return _searchTaskqueueRule.phaseTwoTaskQueue;
    }
}

} // namespace turing
} // namespace isearch
