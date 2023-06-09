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
#include "ha3/turing/ops/RunModelOp.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "autil/SharedObjectMap.h"
#include "turing_ops_util/util/OpUtil.h"
#include "turing_ops_util/util/AttributeToTensor.h"
#include "turing_ops_util/util/ReferenceToTensor.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "suez/turing/common/PoolConfig.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/metrics/BasicBizMetrics.h"
#include "suez/turing/search/RemoteRequestGenerator.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/common/VirtualAttributeClause.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/util/TypeDefine.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/proto/ProtoClassUtil.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/ProviderBase.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/service/QrsModelHandler.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/common/Ha3BizMeta.h"
#include "autil/Log.h"

using namespace tensorflow;
using namespace std;
using namespace autil;
using namespace suez::turing;
using namespace multi_call;
using namespace matchdoc ;

using namespace isearch::common;
using namespace isearch::search;

namespace isearch {
namespace turing {

static const string RUN_GRAPH_METHOD_NAME = "runGraph";
static const string RUN_GRAPH_TARGET_NODE = "run_done";
static const double RUN_GRAPH_FACTOR = 0.9;
static const string RUN_GRAPH_TASK_QUEUE_NAME = "model_task_queue";

#define SET_RESULT()                                                    \
    {                                                                   \
        Tensor *matchDocs = nullptr;                                    \
        OP_REQUIRES_OK_ASYNC(ctx, ctx->allocate_output(0, {}, &matchDocs), done); \
        matchDocs->scalar<Variant>()() = *matchDocsVariant;             \
    }


#define OUTPUT_AND_DONE(CALLBACK)               \
    {                                           \
        SET_RESULT();                           \
        (CALLBACK)();                           \
        return;                                 \
    }

void RunModelOp::ComputeAsync(tensorflow::OpKernelContext *ctx, DoneCallback done) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    auto timeoutTerminator = queryResource->getTimeoutTerminator();
    OP_REQUIRES_ASYNC(ctx, timeoutTerminator,
                      errors::Unavailable("get timeoutTerminator failed"), done);
    int64_t timeout = timeoutTerminator->getLeftTime() / 1000 * _timeoutRatio;
    OP_REQUIRES_ASYNC(ctx, timeout > 0,
                      errors::Unavailable("query timeout."), done);
    auto querySession = queryResource->getGigQuerySession();
    OP_REQUIRES_ASYNC(ctx, querySession,
                      errors::Unavailable("unexpected get querySession failed"), done);
    auto indexSnapshot = queryResource->getIndexSnapshot();
    auto *tracer = queryResource->getTracer();
    REQUEST_TRACE_WITH_TRACER(tracer, INFO, "begin run model op");
    auto searcherQueryResource = dynamic_cast<SearcherQueryResource *>(queryResource);
    OP_REQUIRES_ASYNC(ctx, searcherQueryResource,
                      errors::Unavailable("cast searcherQueryResource failed"), done);
    SearchPartitionResource *partitionResource =
        searcherQueryResource->partitionResource.get();
    OP_REQUIRES_ASYNC(ctx, partitionResource,
                      errors::Unavailable("get partition resource failed."), done);
    const std::string &mainTable = partitionResource->mainTable;
    Ha3BizMeta *ha3BizMeta = searcherQueryResource->ha3BizMeta;
    OP_REQUIRES_ASYNC(ctx, ha3BizMeta, errors::Unavailable("get ha3BizMeta failed"), done);
    auto pool = searcherQueryResource->getPool();
    OP_REQUIRES_ASYNC(ctx, pool, errors::Unavailable("get pool failed"), done);
    map<string, ModelConfig> *modelConfigMap = ha3BizMeta->getModelConfigMap();

    auto *metricReporter = queryResource->getQueryMetricsReporter();

    auto &requestTensor = ctx->input(0).scalar<Variant>()();
    auto requestVariant = requestTensor.get<Ha3RequestVariant>();
    OP_REQUIRES_ASYNC(ctx, requestVariant,
                      errors::Unavailable("get requestVariant failed"), done);
    isearch::common::Request *request = requestVariant->getRequest().get();
    const map<string, string> &kvPairs = request->getConfigClause()->getKVPairs();

    auto &matchDocsTensor = ctx->input(1).scalar<Variant>()();
    auto *matchDocsVariant = matchDocsTensor.get<MatchDocsVariant>();
    OP_REQUIRES_ASYNC(ctx, matchDocsVariant,
                      errors::Unavailable("get matchDocsVariant failed"), done);

    auto requestType = querySession->getRequestType();
    SearchParam param;
    if (!prepareSearchParam(kvPairs, getSourceId(request),
                            modelConfigMap, indexSnapshot,
                            pool, timeout, matchDocsVariant,
                            requestType, mainTable, tracer, param))
    {
        REQUEST_TRACE_WITH_TRACER(tracer, ERROR, "run model op prepare search param failed");
        if (metricReporter) {
            metricReporter->report(1, "user.runModelOp.errorQps", kmonitor::QPS, &_tags);
        }
        OUTPUT_AND_DONE(done);
    }
    if (param.generatorVec.empty()) {
        REQUEST_TRACE_WITH_TRACER(tracer, INFO, "skip run model op");
        OUTPUT_AND_DONE(done);
    }
    param.closure = new RunModelClosure(ctx, std::move(done),
            modelConfigMap, matchDocsVariant, tracer, &_tags, metricReporter);
    REQUEST_TRACE_WITH_TRACER(tracer, TRACE1, "send graph request");
    querySession->call(param, ((RunModelClosure *)param.closure)->getReply());
}

bool RunModelOp::prepareSearchParam(
        const map<string, string> &kvPairs,
        const SourceIdTy &sourceId,
        map<string, ModelConfig> *modelConfigMap,
        indexlib::partition::PartitionReaderSnapshot *indexSnapshot,
        autil::mem_pool::Pool *pool,
        int64_t timeout,
        const MatchDocsVariant *matchDocsVariant,
        multi_call::RequestType requestType,
        const std::string &mainTable,
        suez::turing::Tracer *tracer,
        SearchParam &param)
{
    vector<std::shared_ptr<RemoteRunGraphRequestGenerator>> generators;
    vector<string> modelBizs;
    getModelBizs(kvPairs, modelBizs);
    for (auto &modelBiz : modelBizs) {
        auto iter = modelConfigMap->find(modelBiz);
        if (iter == modelConfigMap->end()) {
            REQUEST_TRACE_WITH_TRACER(tracer, ERROR,
                    "unexpected get [%s] model metadata failed", modelBiz.c_str());
            return false;
        }
        const ModelConfig &modelConfig = iter->second;
        if (modelConfig.modelType != MODEL_TYPE::SCORE_MODEL) {
            continue;
        }
        auto *request = prepareRequest(modelBiz, kvPairs, pool, timeout,
                modelConfig, indexSnapshot, matchDocsVariant, mainTable, tracer);
        if (!request) {
            REQUEST_TRACE_WITH_TRACER(tracer, ERROR, "prepare request failed");
            return false;
        }
        const std::string &localBizName =
            LocalBizService::createLocalBizName(modelBiz);
        std::shared_ptr<RemoteRunGraphRequestGenerator> generator(
                new RemoteRunGraphRequestGenerator(
                        localBizName, RUN_GRAPH_METHOD_NAME, timeout, request, sourceId));
        generator->setUserRequestType(requestType);
        generators.push_back(generator);
    }
    for (auto generator : generators) {
        param.generatorVec.push_back(generator);
    }
    return true;
}

SourceIdTy RunModelOp::getSourceId(const isearch::common::Request *request) {
    auto sourceId = multi_call::INVALID_SOURCE_ID;
    const auto &sourceIdStr = request->getConfigClause()->getSourceId();
    if (!sourceIdStr.empty()) {
        sourceId = autil::HashAlgorithm::hashString64(sourceIdStr.c_str());
    }
    request->setRowKey(sourceId);
    return sourceId;
}

GraphRequest *RunModelOp::prepareRequest(
        const std::string &modelBiz,
        const map<string, string> &kvPairs,
        autil::mem_pool::Pool *pool,
        int64_t timeout,
        const ModelConfig &modelConfig,
        indexlib::partition::PartitionReaderSnapshot *indexSnapshot,
        const MatchDocsVariant *matchDocsVariant,
        const std::string &mainTable,
        suez::turing::Tracer *tracer)
{
    GraphRequest *graphRequest(new GraphRequest());
    graphRequest->set_bizname(modelBiz);
    graphRequest->set_taskqueuename(RUN_GRAPH_TASK_QUEUE_NAME);
    graphRequest->set_timeout(timeout  *RUN_GRAPH_FACTOR);

    GraphInfo &graphInfo = *graphRequest->mutable_graphinfo();

    for (auto &input : modelConfig.searcherModelInfo.inputs) {
        Tensor inputTensor;
        if (extractInputFromKvPairs(modelBiz, kvPairs, input, pool, tracer, inputTensor)) {
        } else if (extractPkVec(input, pool, matchDocsVariant,
                                indexSnapshot, mainTable, tracer, inputTensor))
        {
        } else {
            REQUEST_TRACE_WITH_TRACER(tracer, ERROR, "extract biz [%s] input [%s] failed",
                    modelBiz.c_str(), input.node.c_str());
            delete graphRequest;
            return nullptr;
        }
        suez::turing::NamedTensorProto &inputRequest = *graphInfo.add_inputs();
        inputRequest.set_name(input.node);
        inputTensor.AsProtoField(inputRequest.mutable_tensor());
    }
    for (const auto &output : modelConfig.searcherModelInfo.outputs) {
        graphInfo.add_fetches(output.node);
    }
    graphInfo.add_targets(RUN_GRAPH_TARGET_NODE);
    auto iter = kvPairs.find(OP_DEBUG_KEY);
    if (iter != kvPairs.end()) {
        string errorMsg;
        if (!isearch::service::QrsModelHandler::makeDebugRunOptions(
                        graphRequest, iter->second, errorMsg))
        {
            REQUEST_TRACE_WITH_TRACER(tracer, ERROR, "%s", errorMsg.c_str());
        }
    }
    return graphRequest;

}

bool RunModelOp::extractInputFromKvPairs(const std::string &modelBiz,
        const map<string, string> &kvPairs,
        const NodeConfig &node,
        autil::mem_pool::Pool *pool,
        suez::turing::Tracer *tracer,
        Tensor &inputTensor)
{
    REQUEST_TRACE_WITH_TRACER(tracer, INFO, "extract [%s] from kvpairs",
                              node.node.c_str());
    DataType type;
    if (!DataTypeFromString(node.type, &type)) {
        REQUEST_TRACE_WITH_TRACER(tracer, ERROR,
                "parse data type from [%s] failed", node.type.c_str());
        return false;
    }
    std::string key = createBizNamePrefix(modelBiz, node.node);
    auto iter = kvPairs.find(key);
    if (iter == kvPairs.end()) {
        REQUEST_TRACE_WITH_TRACER(tracer, INFO, "not find [%s] in kvpair, use [%s]",
                key.c_str(), node.node.c_str());
        if (!extractKvpairVariant(kvPairs, node, type, pool, tracer, inputTensor)) {
            return false;
        }
        return true;
    } else {
        if (!extractTensor(iter->second, type, tracer, inputTensor)) {
            return false;
        }
    }
    return true;
}

bool RunModelOp::extractKvpairVariant(
        const map<string, string> &kvPairs,
        const NodeConfig &node,
        const DataType &type,
        autil::mem_pool::Pool *pool,
        suez::turing::Tracer *tracer,
        Tensor &inputTensor)
{
    if (type != DT_VARIANT) {
        REQUEST_TRACE_WITH_TRACER(tracer, ERROR,
                "not supported tensor type [%s] in kvpair", DataTypeString(type).c_str());
        return false;
    }
    auto iter = kvPairs.find(node.node);
    if (iter == kvPairs.end()) {
        REQUEST_TRACE_WITH_TRACER(tracer, INFO,
                "not find input [%s] in kvpair", node.node.c_str());
        return false;
    }
    std::map<std::string, std::string> inputKvPair({{node.node, iter->second}});
    inputTensor = Tensor(DT_VARIANT, TensorShape({}));
    inputTensor.scalar<Variant>()() = KvPairVariant(inputKvPair, pool);
    return true;
}

bool RunModelOp::extractTensor(const std::string &tensorProtoStr,
                               DataType type,
                               suez::turing::Tracer *tracer,
                               Tensor &inputTensor)
{
    TensorProto tensorProto;
    if (!tensorProto.ParseFromString(tensorProtoStr)) {
        REQUEST_TRACE_WITH_TRACER(tracer, ERROR, "parse tensor from kvpair [%s] failed",
                tensorProtoStr.c_str());
        return false;
    }
    if (!inputTensor.FromProto(tensorProto)) {
        REQUEST_TRACE_WITH_TRACER(tracer, ERROR, "parse tensor from proto failed");
        return false;
    }
    if (type != inputTensor.dtype()) {
        REQUEST_TRACE_WITH_TRACER(tracer, ERROR,
                "expect type: [%s], actual type: [%s]",
                DataTypeString(type).c_str(),
                DataTypeString(inputTensor.dtype()).c_str());
        return false;
    }
    return true;
}

bool RunModelOp::extractPkVec(const NodeConfig &node,
                              autil::mem_pool::Pool *pool,
                              const MatchDocsVariant *matchDocsVariant,
                              indexlib::partition::PartitionReaderSnapshot *indexSnapshot,
                              const std::string &mainTable,
                              suez::turing::Tracer *tracer,
                              Tensor &inputTensor)
{
    DataType type;
    if (!DataTypeFromString(node.type, &type)) {
        REQUEST_TRACE_WITH_TRACER(tracer, ERROR,
                "parse data type from [%s] failed", node.type.c_str());
        return false;
    }

    auto matchDocAllocator = matchDocsVariant->getAllocator();
    const auto &matchDocs = matchDocsVariant->getMatchDocs();

    std::unique_ptr<tensorflow::Tensor> tensorPtr;
    REQUEST_TRACE_WITH_TRACER(tracer, INFO, "extract [%s] from attribute",
                              node.node.c_str());
    auto status = AttributeToTensor::fillAttribute(
            pool, matchDocs.data(), matchDocs.size(), mainTable + ":" + node.node,
            indexSnapshot, &tensorPtr);
    if (status.ok()) {
        inputTensor = *tensorPtr;
        if (type != inputTensor.dtype()) {
            REQUEST_TRACE_WITH_TRACER(tracer, ERROR,
                    "expect type: [%s], actual type: [%s]",
                    DataTypeString(type).c_str(),
                    DataTypeString(inputTensor.dtype()).c_str());
            return false;
        }
        return true;
    } else {
        REQUEST_TRACE_WITH_TRACER(tracer, INFO,
                "extract [%s] from table [%s] failed, error msg: %s",
                node.node.c_str(), mainTable.c_str(), status.error_message().c_str());
    }

    auto ref = matchDocAllocator->findReferenceWithoutType(node.node);
    if (ref) {
        REQUEST_TRACE_WITH_TRACER(tracer, INFO, "extract [%s] from matchdoc",
                node.node.c_str());
        auto status = ReferenceToTensor::convertToTensor(
                ref, matchDocs, type, inputTensor);
        if (!status.ok()) {
            REQUEST_TRACE_WITH_TRACER(tracer, WARN,
                    "fill pk [%s] from matchdocs failed, error msg [%s]",
                    node.node.c_str(), status.error_message().c_str());
            return false;
        } else {
            return true;
        }
    } else {
        REQUEST_TRACE_WITH_TRACER(tracer, INFO, "extract [%s] from matchdoc failed",
                node.node.c_str());
        return false;
    }
}

std::string RunModelOp::createBizNamePrefix(const string &bizName, const string &str) {
    return bizName + "." + str;
}

void RunModelOp::getModelBizs(const map<string, string> &kvPairs, vector<string> &modelBizs) {
    auto it = kvPairs.find("model_bizs");
    if (it != kvPairs.end()) {
        const std::string &modelBizsStr = it->second;
        autil::StringUtil::split(modelBizs, modelBizsStr, ';');
    }
}

REGISTER_KERNEL_BUILDER(Name("RunModelOp")
                        .Device(DEVICE_CPU),
                        RunModelOp);

} // namespace builtin
} // namespace isearch
