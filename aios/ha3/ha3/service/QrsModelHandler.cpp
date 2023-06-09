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
#include "ha3/service/QrsModelHandler.h"

#include <Eigen/src/Core/util/Macros.h>
#ifndef AIOS_OPEN_SOURCE
#include <Span.h>
#else
#include <aios/network/opentelemetry/core/Span.h>
#endif
#include <google/protobuf/message.h>
#include <algorithm>
#include <cstddef>
#include <utility>

#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/base64.h"
#include "autil/legacy/exception.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/turing/common/ModelConfig.h"
#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "suez/turing/proto/Search.pb.h"
#include "suez/turing/search/LocalBizService.h"
#include "suez/turing/search/RemoteRequestGenerator.h"
#include "suez/turing/search/RemoteResponse.h"
#include "tensorflow/core/framework/graph_def_util.h"
#include "tensorflow/core/framework/resource_handle.pb.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor.pb.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/platform/types.h"
#include "tensorflow/core/platform/types.h"
#include "turing_ops_util/variant/KvPairVariant.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace tensorflow {
class RunOptions;
}  // namespace tensorflow

using namespace std;
using namespace autil::legacy;
using namespace suez::turing;
using namespace multi_call;
using namespace tensorflow;

using namespace isearch::turing;
using namespace isearch::common;

namespace isearch {
namespace service {

AUTIL_LOG_SETUP(ha3, QrsModelHandler);

static const std::string RUN_GRAPH_METHOD_NAME = "runGraph";
static const std::string RUN_GRAPH_TARGET_NODE = "run_done";

bool QrsModelHandler::process(const vector<std::string> &modelBizs,
                              const vector<map<string, string>> &kvPairs,
                              multi_call::SourceIdTy sourceId,
                              map<string, ModelConfig> *modelConfigMap,
                              autil::mem_pool::Pool *pool,
                              multi_call::QuerySession *querySession,
                              bool needEncodeResult,
                              std::vector<std::map<std::string, std::string>> &result)
{
    bool useSameKvpair = false;
    if (kvPairs.size() == 1) {
        useSameKvpair = true;
    }
    if (!useSameKvpair && modelBizs.size() != kvPairs.size()) {
        REQUEST_TRACE(ERROR, "unexpected not all modelBizs has request kvpair");
        return false;
    }
    // 准备remote request
    REQUEST_TRACE(TRACE1, "prepare graph request");

    vector<std::shared_ptr<RemoteRunGraphRequestGenerator>> generators;
    for (size_t i = 0; i < modelBizs.size(); i++) {
        auto iter = modelConfigMap->find(modelBizs[i]);
        if (iter == modelConfigMap->end()) {
            REQUEST_TRACE(ERROR, "not find biz [%s] model meta", modelBizs[i].c_str());
            return false;
        }
        const auto &qrsModelInfo = iter->second.qrsModelInfo;
        if (qrsModelInfo.empty()) {
            REQUEST_TRACE(TRACE1, "model [%s] do not need run qrs model",
                          modelBizs[i].c_str());
            continue;
        }
        auto *request = prepareRequest(
                modelBizs[i], useSameKvpair ? kvPairs[0] : kvPairs[i], qrsModelInfo, pool);
        if (!request) {
            REQUEST_TRACE(ERROR, "prepare request failed");
            return false;
        }
        const std::string &localBizName =
            suez::turing::LocalBizService::createLocalBizName(modelBizs[i]);
        std::shared_ptr<RemoteRunGraphRequestGenerator> generator(
                new RemoteRunGraphRequestGenerator(
                        localBizName, RUN_GRAPH_METHOD_NAME, _timeout, request, sourceId));
        addBenchMark(querySession, modelBizs[i], generator);
        generator->setUserRequestType(querySession->getRequestType());
        generators.push_back(generator);
    }

    if (generators.empty()) {
        REQUEST_TRACE(INFO, "no graph request generator generated");
        if (!fillOutputTensor(modelBizs, modelConfigMap, {}, needEncodeResult, result)) {
            return false;
        }
        return true;
    }
    // 发送remote request，得到remote response
    SearchParam param;
    for (auto generator : generators) {
        param.generatorVec.push_back(generator);
    }
    REQUEST_TRACE(TRACE1, "send graph request");
    multi_call::ReplyPtr reply;
    querySession->call(param, reply);
    REQUEST_TRACE(TRACE1, "fill result");
    if (!fillResult(modelBizs, modelConfigMap, reply, needEncodeResult, result)) {
        REQUEST_TRACE(TRACE1, "fill result failed");
        return false;
    }
    return true;
}

void QrsModelHandler::addBenchMark(multi_call::QuerySession *querySession,
                                   const std::string &modelBiz,
                                   multi_call::RequestGeneratorPtr generator)
{
    string benchMarkKey = querySession->getUserData(HA_RESERVED_BENCHMARK_KEY);
    if (HA_RESERVED_BENCHMARK_VALUE_1 == benchMarkKey
        || HA_RESERVED_BENCHMARK_VALUE_2 == benchMarkKey) {
        generator->setFlowControlStrategy(getBenchmarkBizName(modelBiz));
    } else {
        generator->setFlowControlStrategy(modelBiz);
    }
}

suez::turing::GraphRequest *QrsModelHandler::prepareRequest(
        const string &bizName,
        const std::map<std::string, std::string> &kvPairs,
        const ModelInfo &qrsModelInfo,
        autil::mem_pool::Pool *pool)
{
    GraphRequest *graphRequest(new GraphRequest());
    graphRequest->set_bizname(bizName);
    graphRequest->set_timeout(_timeout);

    suez::turing::GraphInfo &graphInfo = *graphRequest->mutable_graphinfo();

    for (const auto &node : qrsModelInfo.inputs) {
        std::string key = bizName + "." + node.node;
        auto pair = kvPairs.find(key);
        if (pair == kvPairs.end()) {
            REQUEST_TRACE(INFO, "find [%s] in kvpair failed, use [%s]",
                          key.c_str(), node.node.c_str());
            pair = kvPairs.find(node.node);
        }
        if (pair != kvPairs.end()) {
            REQUEST_TRACE(TRACE1,
                          "use [%s] construct input [%s] biz model [%s] kvpair input",
                          pair->second.c_str(), node.node.c_str(), bizName.c_str());
            suez::turing::NamedTensorProto &inputRequest = *graphInfo.add_inputs();
            auto inputRequestTensor = Tensor(DT_VARIANT, TensorShape({}));
            std::map<std::string, std::string> inputKvPair({{node.node, pair->second}});
            inputRequestTensor.scalar<Variant>()() = KvPairVariant(inputKvPair, pool);
            inputRequest.set_name(node.node);
            inputRequestTensor.AsProtoField(inputRequest.mutable_tensor());
        } else {
            REQUEST_TRACE(ERROR, "not find biz [%s] input node [%s]",
                          bizName.c_str(), node.node.c_str());
            delete graphRequest;
            return nullptr;
        }
    }
    for (const auto &node : qrsModelInfo.outputs) {
        graphInfo.add_fetches(node.node);
    }
    graphInfo.add_targets(RUN_GRAPH_TARGET_NODE);
    auto iter = kvPairs.find(OP_DEBUG_KEY);
    if (iter != kvPairs.end()) {
        string errorMsg;
        if (!makeDebugRunOptions(graphRequest, iter->second, errorMsg)) {
            REQUEST_TRACE(ERROR, "%s", errorMsg.c_str());
        }
    }
    return graphRequest;
}

bool QrsModelHandler::fillResult(const std::vector<std::string> &modelBizs,
                                 std::map<std::string, ModelConfig> *modelConfigMap,
                                 const multi_call::ReplyPtr &reply,
                                 bool needEncodeResult,
                                 std::vector<std::map<std::string, std::string>> &result)
{
    if (reply == nullptr) {
        REQUEST_TRACE(ERROR, "GigReply is null");
        return false;
    }
    size_t lackCount = 0;
    const auto &allResponses = reply->getResponses(lackCount);
    if (allResponses.empty()) {
        REQUEST_TRACE(ERROR, "remote responses is empty");
    }

    map<string, GraphResponse *> resultMap;
    for (auto &response : allResponses) {
        if (!fillResponse(response, resultMap)) {
            REQUEST_TRACE(ERROR, "fill response failed");
        }
    }
    if (!fillOutputTensor(modelBizs, modelConfigMap, resultMap, needEncodeResult, result)) {
        REQUEST_TRACE(ERROR, "fill output tensor failed");
        return false;
    }
    return true;
}

bool QrsModelHandler::fillResponse(const ResponsePtr &response,
                                   map<string, GraphResponse *> &resultMap)
{
    if (response->isFailed()) {
        REQUEST_TRACE(ERROR, "response has error [%s]", response->errorString().c_str());
        return false;
    }

    auto remoteResponse = dynamic_cast<RemoteGraphResponse *>(response.get());
    if (!remoteResponse) {
        REQUEST_TRACE(ERROR, "unexpected cast RemoteGraphResponse failed");
        return false;
    }
    auto message = dynamic_cast<GraphResponse *>(remoteResponse->getMessage());
    if (!message) {
        REQUEST_TRACE(ERROR, "unexpected cast graphResponse failed");
        return false;
    }
    const auto &bizName =
        LocalBizService::getBizNameFromLocalBiz(remoteResponse->getBizName());
    resultMap[bizName] = message;
    return true;
}

bool QrsModelHandler::fillOutputTensor(const vector<string> &modelBizs,
                                       map<string, ModelConfig> *modelConfigMap,
                                       const map<string, GraphResponse *> &resultMap,
                                       bool needEncodeResult,
                                       vector<map<string, string>> &result)
{
    for (size_t i = 0; i < modelBizs.size(); ++i) {
        const string &bizName = modelBizs[i];
        auto iter = modelConfigMap->find(bizName);
        if (iter == modelConfigMap->end()) {
            REQUEST_TRACE(ERROR, "unexpected biz name [%s] not in model meta",
                          bizName.c_str());
            return false;
        }
        const auto &modelConfig = iter->second;
        if (modelConfig.qrsModelInfo.empty()) {
            result.push_back({});
            continue;
        }
        int outputSize = modelConfig.qrsModelInfo.outputs.size();
        GraphResponse *message = nullptr;
        vector<suez::turing::NamedTensorProto> defaultNamedTensorProto;
        auto iterResult = resultMap.find(bizName);
        if (iterResult == resultMap.end()) {
            REQUEST_TRACE(WARN, "no user model result for biz [%s], use default param",
                          bizName.c_str());
            for (const auto &node : modelConfig.qrsModelInfo.outputs) {
                if (!addDefaultParam(node, defaultNamedTensorProto)) {
                    REQUEST_TRACE(ERROR, "add default param failed");
                    return false;
                }
            }
            if (outputSize != defaultNamedTensorProto.size()) {
                REQUEST_TRACE(ERROR,
                        "unexpected default param size [%lu] not equal meta outputs size[%d]",
                        defaultNamedTensorProto.size(), outputSize);
                return false;
            }
        } else {
            message = iterResult->second;
            if (outputSize != message->outputs_size()) {
                REQUEST_TRACE(ERROR,
                        "fetch outputs size [%d] not equal meta outputs size [%d]",
                        message->outputs_size(), outputSize);
                return false;
            }
        }
        if (message) {
            REQUEST_TRACE(DEBUG, "[run model trace] %s", message->debuginfo().c_str());
        }
        map<string, string> outputMap;
        for (int i = 0; i < outputSize; ++i) {
            const suez::turing::NamedTensorProto &namedTensorProto =
                message ? message->outputs(i) : defaultNamedTensorProto[i];
            if (modelConfig.qrsModelInfo.outputs[i].node != namedTensorProto.name()) {
                REQUEST_TRACE(ERROR, "fetch outputs node [%s] not equal meta node [%s]",
                        namedTensorProto.name().c_str(),
                        modelConfig.qrsModelInfo.outputs[i].node.c_str());
                return false;
            }
            std::string tensorProtoStr;
            namedTensorProto.tensor().SerializeToString(&tensorProtoStr);
            outputMap[namedTensorProto.name()] =
                needEncodeResult ?
                autil::legacy::Base64EncodeFast(tensorProtoStr) : std::move(tensorProtoStr);
        }
        result.emplace_back(outputMap);
    }
    return true;
}

bool QrsModelHandler::addDefaultParam(
        const NodeConfig &node,
        vector<suez::turing::NamedTensorProto> &defaultNamedTensorProto)
{
    DataType dtype;
    if (!DataTypeFromString(node.type, &dtype)) {
        REQUEST_TRACE(ERROR, "parse data type from [%s] failed", node.type.c_str());
        return false;
    }
    if (node.shape.size() > 2 || node.shape.size() == 0) {
        REQUEST_TRACE(ERROR, "not support shape dims[%lu] > 2 or = 0", node.shape.size());
        return false;
    }
    if (node.defaultParams.empty()) {
        REQUEST_TRACE(ERROR, "defaultParams is empty");
        return false;
    }
    int defaultParamsSize = node.defaultParams.size();
    int shape = node.shape.size() == 1 ? defaultParamsSize : node.shape.back();
    if (shape <= 0) {
        REQUEST_TRACE(ERROR, "shape [%d] must > 0", shape);
        return false;
    }
    if (defaultParamsSize % shape != 0) {
        REQUEST_TRACE(ERROR, "defaultParams size[%d] can not transfer shape[%d]",
                      defaultParamsSize, shape);
        return false;
    }
    int dims = defaultParamsSize / shape;
#define CASE_MACRO(label, T)                                            \
    case label: {                                                       \
        suez::turing::NamedTensorProto proto;                           \
        proto.set_name(node.node);                                      \
        if (dims == 1) {                                                \
            Tensor t(label, TensorShape({shape}));                      \
            try {                                                       \
                for (size_t i = 0; i < shape; ++i) {                    \
                    t.tensor<T, 1>()(i) = AnyNumberCast<T>(node.defaultParams[i]); \
                }                                                       \
            }  catch (const std::exception &e) {                        \
                REQUEST_TRACE(WARN, "cast failed");                     \
                return false;                                           \
            }                                                           \
            t.AsProtoField(proto.mutable_tensor());                     \
        } else {                                                        \
            Tensor t(label, TensorShape({dims, shape}));                \
            try {                                                       \
                for (size_t i = 0, k = 0; i < dims; ++i) {              \
                    for (size_t j = 0; j < shape; ++j, ++k) {           \
                        t.tensor<T, 2>()(i, j) = AnyNumberCast<T>(node.defaultParams[k]); \
                    }                                                   \
                }                                                       \
            }  catch (const std::exception &e) {                        \
                REQUEST_TRACE(WARN, "cast failed");                     \
                return false;                                           \
            }                                                           \
            t.AsProtoField(proto.mutable_tensor());                     \
        }                                                               \
        defaultNamedTensorProto.emplace_back(proto);                    \
        break;                                                          \
    }

    switch (dtype) {
        CASE_MACRO(DT_INT32, int32_t);
        CASE_MACRO(DT_INT64, int64);
        CASE_MACRO(DT_DOUBLE, double);
        CASE_MACRO(DT_FLOAT, float);
    case DT_STRING: {
        suez::turing::NamedTensorProto proto;
        proto.set_name(node.node);
        if (dims == 1) {
            Tensor t(DT_STRING, TensorShape({shape}));
            try {
                for (size_t i = 0; i < shape; ++i) {
                    t.tensor<std::string, 1>()(i) =
                        AnyCast<std::string>(node.defaultParams[i]);
                }
            }  catch (const std::exception &e) {
                REQUEST_TRACE(WARN, "cast failed");
                return false;
            }
            t.AsProtoField(proto.mutable_tensor());
        } else {
            Tensor t(DT_STRING, TensorShape({dims, shape}));
            try {
                for (size_t i = 0, k = 0; i < dims; ++i) {
                    for (size_t j = 0; j < shape; ++j, ++k) {
                        t.tensor<std::string, 2>()(i, j) =
                            AnyCast<std::string>(node.defaultParams[k]);
                    }
                }
            }  catch (const std::exception &e) {
                REQUEST_TRACE(WARN, "cast failed");
                return false;
            }
            t.AsProtoField(proto.mutable_tensor());
        }
        defaultNamedTensorProto.emplace_back(proto);
        break;
    }
    default: {
        REQUEST_TRACE(ERROR, "not support data type [%s]", node.type.c_str());
        return false;
    }
    }
#undef CASE_MACRO
    return true;
}

bool QrsModelHandler::makeDebugRunOptions(suez::turing::GraphRequest *graphRequest,
        const string &opDebugStr, string &errorMsg)
{
    tensorflow::RunOptions *runOptions = graphRequest->mutable_runoptions();
    const vector<string> &debugInfos = autil::StringUtil::split(opDebugStr, ",");
    for (size_t i = 0; i < debugInfos.size(); i++) {
        vector<string> nodeSlot = autil::StringUtil::split(debugInfos[i], ":");
        if (nodeSlot.size() == 1) {
            AddDebugWatchOpts(nodeSlot[0], 0, runOptions);
        } else if (nodeSlot.size() == 2) {
            AddDebugWatchOpts(nodeSlot[0], stoi(nodeSlot[1]), runOptions);
        } else {
            errorMsg = "error: not supported debug op name [" + debugInfos[i] + "]";
            return false;
        }
    }
    return true;
}
} // namespace service
} // namespace isearch
