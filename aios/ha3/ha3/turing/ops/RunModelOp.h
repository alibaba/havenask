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
#include <google/protobuf/message.h>
#include <stddef.h>
#include <stdint.h>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/Scope.h"
#include "autil/TimeUtility.h"
#include "ha3/turing/common/ModelConfig.h"
#include "ha3/turing/ops/ha3_op_common.h"
#include "indexlib/misc/common.h"
#include "kmonitor/client/MetricType.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "matchdoc/CommonDefine.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Closure.h"
#include "aios/network/gig/multi_call/interface/Reply.h"
#include "aios/network/gig/multi_call/interface/Response.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/proto/Search.pb.h"
#include "suez/turing/search/LocalBizService.h"
#include "suez/turing/search/RemoteResponse.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/status.h"
#include "turing_ops_util/util/OpUtil.h"
#include "turing_ops_util/util/TensorToMatchDoc.h"
#include "turing_ops_util/variant/MatchDocsVariant.h"
#include "turing_ops_util/variant/Tracer.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
}
}

namespace isearch {
namespace common {
class Request;
} // namespace common
} // namespace isearch

namespace isearch {
namespace turing {

class RunModelOp : public tensorflow::AsyncOpKernel
{
private:
    class RunModelClosure : public multi_call::Closure {
    public:
        RunModelClosure(OpKernelContext *ctx,
                        AsyncOpKernel::DoneCallback done,
                        std::map<std::string, ModelConfig> *modelConfigMap,
                        const suez::turing::MatchDocsVariant *matchDocsVariant,
                        suez::turing::Tracer *tracer,
                        kmonitor::MetricsTags *tags,
                        kmonitor::MetricsReporter *metricReporter)
            : _ctx(ctx),
              _modelConfigMap(modelConfigMap),
              _matchDocsVariant(nullptr),
              _tracer(tracer),
              _tags(tags),
              _metricReporter(metricReporter)
        {
            if (matchDocsVariant != nullptr) {
                _matchDocsVariant = new suez::turing::MatchDocsVariant(*matchDocsVariant);
            }
            _done = std::move(done);
        }

        ~RunModelClosure() {
            if (_matchDocsVariant != nullptr) {
                delete _matchDocsVariant;
            }
        }

        void Run() override {
            auto runner = _ctx->runner();
            if (runner == nullptr) {
                doRun();
            } else {
                auto closure = [this]() { doRun(); };
                (*runner)(closure);
            }
        }

        void doRun() {
            autil::ScopeGuard deleteThis([this]() { delete this; });
            OP_REQUIRES_ASYNC(_ctx, _matchDocsVariant,
                    errors::Unavailable("allocate matchDocsVariant failed"), _done);
            auto latency = _waitLatency.done_ms();
            if (_metricReporter) {
                _metricReporter->report(latency, "user.runModelOp.runModelLatency",
                        kmonitor::GAUGE, _tags);
            }
            REQUEST_TRACE_WITH_TRACER(_tracer, INFO, "run model latency [%lf] ms",
                    latency);
            if (!fillResult()) {
                if (_metricReporter) {
                    _metricReporter->report(1, "user.runModelOp.errorQps", kmonitor::QPS,
                            _tags);
                }
            }
            REQUEST_TRACE_WITH_TRACER(_tracer, INFO, "end run model op");
            tensorflow::Tensor *matchDocs = nullptr;
            OP_REQUIRES_OK_ASYNC(_ctx, _ctx->allocate_output(0, {}, &matchDocs), _done);
            matchDocs->scalar<Variant>()() = *_matchDocsVariant;
            delete _matchDocsVariant;
            _matchDocsVariant = nullptr;
            _done();
        }
        bool fillResult() {
            autil::ScopedTime2 fillResultLatency;
            size_t lackCount = 0;
            if (_reply == nullptr) {
                REQUEST_TRACE_WITH_TRACER(_tracer, ERROR, "GigReply is null");
                return false;
            }
            const auto &allResponses = _reply->getResponses(lackCount);
            if (allResponses.empty()) {
                REQUEST_TRACE_WITH_TRACER(_tracer, ERROR, "remote responses is empty");
                return false;
            }
            for (auto &response : allResponses) {
                if (!fillOutputTensor(response)) {
                    REQUEST_TRACE_WITH_TRACER(_tracer, ERROR, "fill output tensor failed");
                    return false;
                }
            }
            auto latency = fillResultLatency.done_ms();
            if (_metricReporter) {
                _metricReporter->report(latency, "user.runModelOp.fillResultLatency",
                        kmonitor::GAUGE, _tags);
            }
            REQUEST_TRACE_WITH_TRACER(_tracer, INFO,
                    "run model fill result Latency [%lf] ms", latency);
            return true;
        }
        bool fillOutputTensor(const multi_call::ResponsePtr &response) {
            if (response->isFailed()) {
                REQUEST_TRACE_WITH_TRACER(_tracer, ERROR, "response has error [%s]",
                        response->errorString().c_str());
                return false;
            }

            auto remoteResponse =
                dynamic_cast<suez::turing::RemoteGraphResponse *>(response.get());
            if (!remoteResponse) {
                REQUEST_TRACE_WITH_TRACER(_tracer, ERROR,
                        "unexpected cast RemoteGraphResponse failed");
                return false;
            }
            auto message =
                dynamic_cast<suez::turing::GraphResponse *>(remoteResponse->getMessage());
            if (!message) {
                REQUEST_TRACE_WITH_TRACER(_tracer, ERROR, "unexpected cast graphResponse failed");
                return false;
            } else {
                REQUEST_TRACE_WITH_TRACER(_tracer, DEBUG, "[run model trace] %s",
                        message->debuginfo().c_str());
            }
            const auto &bizName = suez::turing::LocalBizService::getBizNameFromLocalBiz(
                    remoteResponse->getBizName());
            auto iter = _modelConfigMap->find(bizName);
            if (iter == _modelConfigMap->end()) {
                REQUEST_TRACE_WITH_TRACER(_tracer, ERROR,
                        "unexpected biz name [%s] not in model meta", bizName.c_str());
                return false;
            }
            const auto &modelConfig = iter->second;
            int outputSize = modelConfig.searcherModelInfo.outputs.size();
            if (outputSize != message->outputs_size()) {
                REQUEST_TRACE_WITH_TRACER(_tracer, ERROR,
                        "fetch outputs size [%d] not equal meta outputs size [%d]",
                        message->outputs_size(), outputSize);
                return false;
            }
            for (int i = 0; i < outputSize; ++i) {
                const suez::turing::NamedTensorProto &tensorProto = message->outputs(i);
                if (modelConfig.searcherModelInfo.outputs[i].node != tensorProto.name()) {
                    REQUEST_TRACE_WITH_TRACER(_tracer, ERROR,
                            "fetch outputs node [%s] not equal meta node [%s]",
                            tensorProto.name().c_str(),
                            modelConfig.searcherModelInfo.outputs[i].node.c_str());
                    return false;
                }
                tensorflow::Tensor outputTensor;
                if (!outputTensor.FromProto(tensorProto.tensor())) {
                    REQUEST_TRACE_WITH_TRACER(_tracer, ERROR,
                            "from proto [%s] failed", tensorProto.name().c_str());
                    return false;
                }
                // todo check type
                REQUEST_TRACE_WITH_TRACER(_tracer, TRACE1, "fill [%s] to matchdocs, type [%s]",
                        (suez::turing::PROVIDER_VAR_NAME_PREFIX + tensorProto.name()).c_str(),
                        modelConfig.searcherModelInfo.outputs[i].type.c_str());
                auto status = suez::turing::TensorToMatchDoc::mountTensorToMatchDoc(
                        _matchDocsVariant,
                        suez::turing::PROVIDER_VAR_NAME_PREFIX + tensorProto.name(),
                        outputTensor, SerializeLevel::SL_NONE, false);
                if (!status.ok()) {
                    REQUEST_TRACE_WITH_TRACER(_tracer, ERROR,
                            "mount tensor to matchdoc failed, error msg [%s]",
                            status.error_message().c_str());
                    return false;
                }
            }
            return true;
        }
        multi_call::ReplyPtr &getReply() { return _reply; }
    private:
        OpKernelContext *_ctx;
        AsyncOpKernel::DoneCallback _done;
        multi_call::ReplyPtr _reply;
        std::map<std::string, ModelConfig> *_modelConfigMap;
        suez::turing::MatchDocsVariant *_matchDocsVariant;
        suez::turing::Tracer *_tracer;
        autil::ScopedTime2 _waitLatency;
        kmonitor::MetricsTags *_tags;
        kmonitor::MetricsReporter *_metricReporter;
    };

public:
    explicit RunModelOp(tensorflow::OpKernelConstruction *ctx)
        : tensorflow::AsyncOpKernel(ctx)
    {
        OP_REQUIRES_OK(ctx, ctx->GetAttr("timeout_ratio", &_timeoutRatio));
        ADD_OP_BASE_TAG(_tags);
    }
    ~RunModelOp() {}
    RunModelOp(const RunModelOp &) = delete;
    RunModelOp& operator=(const RunModelOp &) = delete;
public:
    void ComputeAsync(tensorflow::OpKernelContext *ctx, DoneCallback done) override;
private:
    bool prepareSearchParam(const std::map<std::string, std::string> &kvPairs,
                            const multi_call::SourceIdTy &sourceId,
                            std::map<std::string, ModelConfig> *modelConfigMap,
                            indexlib::partition::PartitionReaderSnapshot *indexSnapshot,
                            autil::mem_pool::Pool *pool,
                            int64_t timeout,
                            const suez::turing::MatchDocsVariant *matchDocsVariant,
                            multi_call::RequestType requestType,
                            const std::string &mainTable,
                            suez::turing::Tracer *tracer,
                            multi_call::SearchParam &param);
    static multi_call::SourceIdTy getSourceId(const isearch::common::Request *request);
    suez::turing::GraphRequest *prepareRequest(const std::string &modelBiz,
            const std::map<std::string, std::string> &kvPairs,
            autil::mem_pool::Pool *pool,
            int64_t timeout,
            const ModelConfig &modelConfig,
            indexlib::partition::PartitionReaderSnapshot *indexSnapshot,
            const suez::turing::MatchDocsVariant *matchDocsVariant,
            const std::string &mainTable,
            suez::turing::Tracer *tracer);
    bool extractInputFromKvPairs(const std::string &modelBiz,
                                 const std::map<std::string, std::string> &kvPairs,
                                 const NodeConfig &node,
                                 autil::mem_pool::Pool *pool,
                                 suez::turing::Tracer *tracer,
                                 tensorflow::Tensor &inputTensor);
    bool extractKvpairVariant(
            const std::map<std::string, std::string> &kvPairs,
            const NodeConfig &node,
            const tensorflow::DataType &type,
            autil::mem_pool::Pool *pool,
            suez::turing::Tracer *tracer,
            tensorflow::Tensor &inputTensor);
    bool extractTensor(const std::string &tensorProtoStr,
                       tensorflow::DataType type,
                       suez::turing::Tracer *tracer,
                       tensorflow::Tensor &inputTensor);
    bool extractPkVec(const NodeConfig &node,
                      autil::mem_pool::Pool *pool,
                      const suez::turing::MatchDocsVariant *matchDocsVariant,
                      indexlib::partition::PartitionReaderSnapshot *indexSnapshot,
                      const std::string &mainTable,
                      suez::turing::Tracer *tracer,
                      tensorflow::Tensor &inputTensor);
    static std::string createBizNamePrefix(
            const std::string &bizName, const std::string &str);
    static void getModelBizs(const std::map<std::string, std::string> &kvPairs,
                             std::vector<std::string> &modelBizs);
private:
    float _timeoutRatio;
    kmonitor::MetricsTags _tags;
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch
