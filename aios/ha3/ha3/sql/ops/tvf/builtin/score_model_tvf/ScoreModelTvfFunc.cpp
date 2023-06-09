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
#include "ha3/sql/ops/tvf/builtin/ScoreModelTvfFunc.h"

#include <google/protobuf/message.h>
#include <cstddef>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/base64.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/access_log/EventTrackLog.h"
#include "ha3/service/QrsModelHandler.h"
#include "ha3/sql/common/KvPairParser.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/tvf/SqlTvfProfileInfo.h"
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/TableToTensor.h"
#include "ha3/sql/ops/tvf/builtin/TensorToTable.h"
#include "ha3/sql/ops/tvf/builtin/SamplingTable.h"
#include "iquan/common/catalog/FunctionCommonDef.h"
#include "iquan/common/catalog/TvfFunctionDef.h"
#include "iquan/common/catalog/TvfFunctionModel.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/interface/RequestGenerator.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "suez/turing/proto/Search.pb.h"
#include "suez/turing/search/LocalBizService.h"
#include "suez/turing/search/RemoteRequestGenerator.h"
#include "suez/turing/search/RemoteResponse.h"
#include "table/Row.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor.pb.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/platform/byte_order.h"
#include "turing_ops_util/variant/KvPairVariant.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"

using namespace tensorflow;
using namespace std;
using namespace autil;
using namespace table;
using namespace suez::turing;
using namespace multi_call;

using namespace isearch::turing;
using namespace isearch::common;

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor
using namespace kmonitor;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, ScoreModelTvfFunc);
AUTIL_LOG_SETUP(sql, ScoreModelTvfFuncCreator);

static const string RUN_GRAPH_METHOD_NAME = "runGraph";
static const string RUN_GRAPH_TARGET_NODE = "run_done";
static const string RUN_GRAPH_TASK_QUEUE_NAME = "model_task_queue";
const string ScoreModelTvfFuncCreator::SCORE_MODEL_RETURNS = "returns";

const string scoreModelTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "scoreModelTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false,
      "physical_type": "model_score",
      "replace_params_num": 1
    },
    "prototypes": [
      {
        "params": {
          "scalars": [
             {
               "type":"string"
             }
         ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        },
        "returns": {
          "new_fields": [
          ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        }
      }
    ]
  }
}
)tvf_json";

// bizName(kvpair, embedding)

class ScoreModelTvfFuncMetrics : public MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "sampling_qps");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, const size_t *uselessValue) {
        REPORT_MUTABLE_QPS(_qps);
    }
private:
    MutableMetric *_qps = nullptr;
};

ScoreModelTvfFunc::ScoreModelTvfFunc(const std::string &modelBiz)
    : _modelBiz(modelBiz)
    , _pool(nullptr)
    , _queryPool(nullptr)
    , _modelConfigMap(nullptr)
    , _querySession(nullptr)
    , _timeout(50) {}

ScoreModelTvfFunc::~ScoreModelTvfFunc() {}

bool ScoreModelTvfFunc::init(TvfFuncInitContext &context) {
    _metricReporter = context.metricReporter;
    if (context.params.size() != 2) {
        SQL_LOG(ERROR, "param size [%ld] not equal 2", context.params.size());
        return false;
    }
    KvPairParser::parse(context.params[1], SQL_MODEL_KV_PAIR_SPLIT, SQL_MODEL_KV_SPLIT,
                        _kvPairs);
    auto iter = _kvPairs.find(QINFO_KEY_FG_USER);
    if (iter != _kvPairs.end()) {
        iter->second = autil::legacy::Base64DecodeFast(iter->second);
    }
    if (!parseEmbeddingTensors(context.params[0])) {
        SQL_LOG(ERROR, "parse embedding tensors failed [%s].", context.params[0].c_str());
        return false;
    }
    iter = _kvPairs.find(QINFO_KEY_RN);
    _rn = "";
    if (iter != _kvPairs.end()) {
        _rn = iter->second;
    }
    _pool = context.poolResource->getPool().get();
    _queryPool = context.queryPool;
    _modelConfigMap = context.modelConfigMap;
    _querySession = context.querySession;

    return true;
}

bool ScoreModelTvfFunc::parseEmbeddingTensors(const std::string &embeddingStr) {
    map<string, string> embedding;
    try {
        if (!embeddingStr.empty()) {
            autil::legacy::FromJsonString(embedding, embeddingStr);
        }
    } catch (...) {
        SQL_LOG(ERROR, "parse embedding str failed [%s].", embeddingStr.c_str());
        return false;
    }
    for (auto iter : embedding) {
        TensorProto tensorProto;
        if (!tensorProto.ParseFromString(autil::legacy::Base64DecodeFast(iter.second))) {
            SQL_LOG(ERROR, "parse tensor from kvpair [%s] failed", iter.second.c_str());
            return false;
        }
        Tensor inputTensor;
        if (!inputTensor.FromProto(tensorProto)) {
            SQL_LOG(ERROR, "parse tensor from proto failed");
            return false;
        }
        _embeddingTensors.insert(make_pair(iter.first, inputTensor));
    }
    return true;
}

bool ScoreModelTvfFunc::doCompute(const TablePtr &input, TablePtr &output) {
    output = input;
    logRecord(input);
    SearchParam param;
    if (!prepareSearchParam(input, param)) {
        return false;
    }
    ReplyPtr reply;
    _querySession->call(param, reply);
    if (!fillResult(reply, output)) {
        return false;
    }
    return true;
}

bool ScoreModelTvfFunc::fillResult(ReplyPtr reply, TablePtr &output) {
    size_t lackCount = 0;
    if (reply == nullptr) {
        SQL_LOG(ERROR, "GigReply is null");
        return false;
    }
    const auto &allResponses = reply->getResponses(lackCount);
    if (allResponses.empty()) {
        SQL_LOG(ERROR, "remote responses is empty");
        return false;
    }
    for (auto &response : allResponses) {
        if (!fillOutputTensor(response, output)) {
            SQL_LOG(ERROR, "fill output tensor failed");
            return false;
        }
    }
    return true;
}

bool ScoreModelTvfFunc::fillOutputTensor(const multi_call::ResponsePtr &response,
                                         TablePtr &output) {
    if (response->isFailed()) {
        SQL_LOG(ERROR, "response has error [%s]", response->errorString().c_str());
        return false;
    }

    auto remoteResponse = dynamic_cast<suez::turing::RemoteGraphResponse *>(response.get());
    if (!remoteResponse) {
        SQL_LOG(ERROR, "unexpected cast RemoteGraphResponse failed");
        return false;
    }
    auto message = dynamic_cast<suez::turing::GraphResponse *>(remoteResponse->getMessage());
    if (!message) {
        SQL_LOG(ERROR, "unexpected cast graphResponse failed");
        return false;
    } else {
        SQL_LOG(DEBUG, "[run model trace] %s", message->debuginfo().c_str());
    }
    const auto &bizName
        = suez::turing::LocalBizService::getBizNameFromLocalBiz(remoteResponse->getBizName());
    auto iter = _modelConfigMap->find(bizName);
    if (iter == _modelConfigMap->end()) {
        SQL_LOG(ERROR, "unexpected biz name [%s] not in model meta", bizName.c_str());
        return false;
    }
    const auto &modelConfig = iter->second;
    int outputSize = modelConfig.searcherModelInfo.outputs.size();
    if (outputSize != message->outputs_size()) {
        SQL_LOG(ERROR,
                "fetch outputs size [%d] not equal meta outputs size [%d]",
                message->outputs_size(),
                outputSize);
        return false;
    }
    for (int i = 0; i < outputSize; ++i) {
        const suez::turing::NamedTensorProto &tensorProto = message->outputs(i);
        const auto &node = modelConfig.searcherModelInfo.outputs[i];
        if (node.node != tensorProto.name()) {
            SQL_LOG(ERROR,
                    "fetch outputs node [%s] not equal meta node [%s]",
                    tensorProto.name().c_str(),
                    node.node.c_str());
            return false;
        }
        tensorflow::Tensor outputTensor;
        if (!outputTensor.FromProto(tensorProto.tensor())) {
            SQL_LOG(ERROR, "from proto [%s] failed", tensorProto.name().c_str());
            return false;
        }
        DataType type;
        if (!DataTypeFromString(node.type, &type)) {
            SQL_LOG(ERROR, "parse data type from [%s] failed", node.type.c_str());
            return false;
        }
        if (outputTensor.dtype() != type) {
            SQL_LOG(ERROR,
                    "output column type [%s] is not equal to tensor type [%s]",
                    DataTypeString(outputTensor.dtype()).c_str(),
                    DataTypeString(type).c_str());
            return false;
        }
        SQL_LOG(TRACE1, "fill [%s] to table, type [%s]", node.node.c_str(), node.type.c_str());
        if (!TensorToTable::convertTensorToColumn(outputTensor, node.node, _queryPool, output)) {
            SQL_LOG(ERROR, "convert tensor to column [%s] failed", node.node.c_str());
            return false;
        }
    }
    return true;
}

bool ScoreModelTvfFunc::prepareSearchParam(const TablePtr &input, SearchParam &param) {
    auto iter = _modelConfigMap->find(_modelBiz);
    if (iter == _modelConfigMap->end()) {
        SQL_LOG(ERROR, "unexpected get [%s] model metadata failed", _modelBiz.c_str());
        return false;
    }
    const ModelConfig &modelConfig = iter->second;
    if (modelConfig.modelType != MODEL_TYPE::SCORE_MODEL) {
        SQL_LOG(ERROR, "model biz[%s] is not score model", _modelBiz.c_str());
        return false;
    }
    auto *request = prepareGraphRequest(_kvPairs, _timeout, modelConfig, input);
    if (!request) {
        SQL_LOG(ERROR, "prepare request failed");
        return false;
    }
    const std::string &localBizName = LocalBizService::createLocalBizName(_modelBiz);
    int64_t sourceId = 0;
    std::shared_ptr<RemoteRunGraphRequestGenerator> generator(new RemoteRunGraphRequestGenerator(
        localBizName, RUN_GRAPH_METHOD_NAME, _timeout, request, sourceId));
    generator->setUserRequestType(_querySession->getRequestType());
    param.generatorVec.push_back(generator);
    return true;
}

suez::turing::GraphRequest *
ScoreModelTvfFunc::prepareGraphRequest(const unordered_map<string, string> &kvPairs,
                                       int64_t timeout,
                                       const ModelConfig &modelConfig,
                                       const TablePtr &tableInput) {
    std::unique_ptr<GraphRequest> graphRequest(new GraphRequest());
    graphRequest->set_bizname(_modelBiz);
    graphRequest->set_taskqueuename(RUN_GRAPH_TASK_QUEUE_NAME);
    graphRequest->set_timeout(timeout);

    suez::turing::GraphInfo &graphInfo = *graphRequest->mutable_graphinfo();

    if (modelConfig.searcherModelInfo.inputs.empty()
        || modelConfig.searcherModelInfo.outputs.empty()) {
        SQL_LOG(ERROR, "unexpected empty inputs or outpus");
        return nullptr;
    }
    for (auto &input : modelConfig.searcherModelInfo.inputs) {
        DataType type;
        if (!DataTypeFromString(input.type, &type)) {
            SQL_LOG(ERROR, "parse data type from [%s] failed", input.type.c_str());
            return nullptr;
        }
        Tensor inputTensor;
        auto iter = _embeddingTensors.find(input.node);
        if (iter != _embeddingTensors.end()) {
            if (type != iter->second.dtype()) {
                SQL_LOG(ERROR,
                        "user embedding expect type: [%s], actual type: [%s]",
                        DataTypeString(type).c_str(),
                        DataTypeString(iter->second.dtype()).c_str());
                return nullptr;
            }
            inputTensor = iter->second;
        } else if (extractInputFromKvPairs(kvPairs, input, type, inputTensor)) {
        } else if (TableToTensor::convertColumnToTensor(
                       tableInput, input.node, type, inputTensor)) {
        } else {
            SQL_LOG(
                ERROR, "extract biz [%s] input [%s] failed", _modelBiz.c_str(), input.node.c_str());
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
                        graphRequest.get(), iter->second, errorMsg))
        {
            SQL_LOG(ERROR, "%s", errorMsg.c_str());
        }
    }
    return graphRequest.release();
}

bool ScoreModelTvfFunc::extractInputFromKvPairs(const unordered_map<string, string> &kvPairs,
                                                const NodeConfig &node,
                                                const DataType &type,
                                                Tensor &inputTensor) {
    SQL_LOG(INFO, "extract [%s] from kvpairs", node.node.c_str());
    if (type != DT_VARIANT) {
        SQL_LOG(DEBUG, "not supported tensor type [%s] in kvpair", DataTypeString(type).c_str());
        return false;
    }
    auto iter = kvPairs.find(node.node);
    if (iter == kvPairs.end()) {
        SQL_LOG(DEBUG, "not find input [%s] in kvpair", node.node.c_str());
        return false;
    }
    std::map<std::string, std::string> inputKvPair({{node.node, iter->second}});
    inputTensor = Tensor(DT_VARIANT, TensorShape({}));
    inputTensor.scalar<Variant>()() = KvPairVariant(inputKvPair, _pool);

    return true;
}

void ScoreModelTvfFunc::logRecord(const TablePtr &input) {
    auto mapIter = _modelConfigMap->find(_modelBiz);
    if (mapIter == _modelConfigMap->end()) {
        SQL_LOG(TRACE1, "unexpected get [%s] model metadata failed", _modelBiz.c_str());
        return;
    }
    const auto &modelConfig = mapIter->second;
    const SamplingInfo &samplingInfo = modelConfig.samplingInfo;

    if (_rn.empty() || !samplingInfo.open()) {
        return;
    }
    std::string content("rn:" + _rn + ";stage:before_score_model;");
    if (!SamplingTable::samplingTableToString(input, samplingInfo, content)) {
        SQL_LOG(TRACE1, "sampling table to string failed");
        return;
    }
    std::string traceId("");
    if (_querySession) {
        auto span = _querySession->getTraceServerSpan();
        if (span) {
            traceId = opentelemetry::EagleeyeUtil::getETraceId(span);
        }
    }
    if (_metricReporter) {
        const string &pathName = "sql.user.ops.tvf.score_model";
        auto reporter = _metricReporter->getSubReporter(pathName, {});
        const size_t uselessValue = 0;
        reporter->report<ScoreModelTvfFuncMetrics>(nullptr, &uselessValue);
    }
    EventTrackLog trackLog(traceId, content);
}


ScoreModelTvfFuncCreator::ScoreModelTvfFuncCreator()
    : TvfFuncCreatorR(scoreModelTvfDef) {}

ScoreModelTvfFuncCreator::~ScoreModelTvfFuncCreator() {}

TvfFunc *ScoreModelTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new ScoreModelTvfFunc(info.tvfName);
}

bool ScoreModelTvfFuncCreator::initTvfModel(iquan::TvfModel &tvfModel,
                                            const isearch::sql::SqlTvfProfileInfo &info) {
    if (!transToTvfReturnType(info.parameters,
                              tvfModel.functionContent.tvfs[0].returns.newFields)) {
        return false;
    }
    return true;
}

bool ScoreModelTvfFuncCreator::transToTvfReturnType(const KeyValueMap &kvMap,
                                                    std::vector<iquan::TvfFieldDef> &newFields) {
    auto iter = kvMap.find(SCORE_MODEL_RETURNS);
    if (iter == kvMap.end()) {
        SQL_LOG(ERROR, "not find [%s] in parameters", SCORE_MODEL_RETURNS.c_str());
        return false;
    }
    std::vector<std::vector<std::string>> outputInfo;
    autil::StringUtil::fromString(iter->second, outputInfo, string(":"), string(","));
    for (auto info : outputInfo) {
        if (info.size() != 2) {
            SQL_LOG(ERROR,
                    "unexpected output info[%s] size [%lu] not equal to 2",
                    iter->second.c_str(),
                    info.size());
            return false;
        }
        std::string type;
        if (info[1] == "uint16" || info[1] == "int16") {
            type = "int16";
        } else if (info[1] == "uint32" || info[1] == "int32") {
            type = "int32";
        } else if (info[1] == "float32") {
            type = "float";
        } else if (info[1] == "float64") {
            type = "double";
        } else if (info[1] == "int64") {
            type = "int64";
        } else {
            SQL_LOG(ERROR, "not support [%s] type", info[1].c_str());
            return false;
        }
        iquan::TvfFieldDef fieldDef(info[0], iquan::ParamTypeDef(false, type));
        newFields.emplace_back(fieldDef);
    }
    return true;
}

REGISTER_RESOURCE(ScoreModelTvfFuncCreator);

} // namespace sql
} // namespace isearch
