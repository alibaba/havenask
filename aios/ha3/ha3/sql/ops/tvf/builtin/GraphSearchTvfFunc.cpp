#include <ha3/sql/ops/tvf/builtin/GraphSearchTvfFunc.h>
#include <ha3/sql/ops/tvf/TvfLocalSearchResource.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <ha3/sql/data/TableUtil.h>
#include <suez/turing/search/GraphServiceImpl.h>
#include <autil/URLUtil.h>
#include <tensorflow/core/framework/types.pb.h>
#include <tensorflow/core/framework/tensor_shape.h>
#include <ha3/queryparser/RequestSymbolDefine.h>
#include <basic_ops/variant/QInfoFieldVariant.h>
#include <basic_variant/variant/RawTensorVariant.h>
using namespace tensorflow;
using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(sql);

const string GraphSearchTvfFunc::INPUT_QINFO_NAME = "qinfo";
const string GraphSearchTvfFunc::INPUT_PK_NAME = "pk";
const string GraphSearchTvfFunc::INPUT_CONTEXT_NAME = "context";
const string GraphSearchTvfFunc::OUTPUT_FETCHES_NAME = "modelscore";
const string GraphSearchTvfFunc::OUTPUT_TARGETS_NAME = "modelscore";
const string GraphSearchTvfFunc::OUTPUT_SCORE_NAME = "__buildin_score__";

const SqlTvfProfileInfo graphSearchTvfInfo("graphSearchTvf", "graphSearchTvf");
const string graphSearchTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "graphSearchTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false
    },
    "prototypes": [
      {
        "params": {
          "scalars": [
             {
               "type":"string"
             },
             {
               "type":"string"
             },
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
            {
              "field_name": "__buildin_score__",
              "field_type": {
                "type": "float"
              }
            }
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

GraphSearchTvfFunc::GraphSearchTvfFunc() {
}

GraphSearchTvfFunc::~GraphSearchTvfFunc() {
}

bool GraphSearchTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 3) {
        SQL_LOG(ERROR, "param size [%ld] not equal 3", context.params.size());
        return false;
    }
    _bizName = context.params[0];
    _pkColumnName = context.params[1];

    std::string qinfoPairsStr = URLUtil::decode(context.params[2]);
    if (!prepareQInfoKvPairsMap(qinfoPairsStr, _qinfoPairsMap)) {
        SQL_LOG(ERROR, "prepare qinfo kvpairs map failed");
        return false;
    }
    auto tvfLocalSearchResource =
        context.tvfResourceContainer->get<TvfLocalSearchResource>("TvfLocalSearchResource");
    if (!tvfLocalSearchResource) {
        SQL_LOG(ERROR, "get tvf local search resource failed");
        return false;
    }

    _localSearchService = tvfLocalSearchResource->getLocalSearchService();
    if (!_localSearchService) {
        SQL_LOG(ERROR, "get local search service failed");
        return false;
    }
    return true;
}

bool GraphSearchTvfFunc::extractPkVec(
        const std::string &pkColumnName, const TablePtr &input,
        vector<int64_t> &pkVec) {
    ColumnData<int64_t> *pkColumn =
        TableUtil::getColumnData<int64_t>(input, pkColumnName);
    if (!pkColumn) {
        SQL_LOG(ERROR, "get pk column[%s] failed", pkColumnName.c_str());
        return false;
    }

    size_t inputRowCount = input->getRowCount();
    for (size_t i = 0; i < inputRowCount; i++) {
        int64_t pk = pkColumn->get(i);
        pkVec.push_back(pk);
    }
    return true;
}

bool GraphSearchTvfFunc::modelScoresToOutputTable(
        const vector<float> &modelScores,
        const string &outputScoreColumnName,
        TablePtr &output) {
    ColumnData<float> *scoreColumn =
        TableUtil::declareAndGetColumnData<float>(output,
                outputScoreColumnName, true);
    if(!scoreColumn) {
        SQL_LOG(ERROR, "declare and get column data[%s] failed",
                outputScoreColumnName.c_str());
        return false;
    }

    size_t outputRowCount = output->getRowCount();
    if (outputRowCount != modelScores.size()) {
        SQL_LOG(ERROR, "output row count[%d] isn't equal to size of model scores[%d]",
                (int32_t)outputRowCount, (int32_t)modelScores.size());
        return false;
    }

    for (size_t i = 0; i < outputRowCount; i++) {
        scoreColumn->set(i, modelScores[i]);
    }
    return true;
}

bool GraphSearchTvfFunc::doCompute(const TablePtr &input, TablePtr &output) {
    output = input;
    suez::turing::GraphRequest graphRequest;
    suez::turing::GraphResponse graphResponse;
    Tensor qinfoTensor;
    Tensor pkTensor;
    Tensor contextTensor;

    prepareQInfoTensor(_qinfoPairsMap, qinfoTensor);

    vector<int64_t> pkVec;
    bool ret = extractPkVec(_pkColumnName, input, pkVec);
    if (!ret) {
        SQL_LOG(ERROR, "extrac pk vec failed");
        return false;
    }


    preparePKTensor(pkVec, pkTensor);
    prepareContextTensor(contextTensor);
    prepareGraphRequest(_bizName, qinfoTensor, pkTensor, contextTensor,
                        graphRequest);

    _localSearchService->runGraph(&graphRequest, &graphResponse);

    if (graphResponse.errorinfo().errorcode() != 0) {
        SQL_LOG(ERROR, "error_code: %d, error_msg: %s",
                (int32_t)graphResponse.errorinfo().errorcode(),
                graphResponse.errorinfo().errormsg().c_str());
        return false;
    }

    vector<float> modelScores;
    ret = getModelScoresFromGraphResponse(graphResponse, modelScores);
    if (!ret) {
        SQL_LOG(ERROR, "get model scores from graph response failed");
        return false;
    }

    if (pkVec.size() != modelScores.size()) {
        SQL_LOG(ERROR, "size of pks[%d] is not equal to size of scores[%d]",
                (int32_t)pkVec.size(),
                (int32_t)modelScores.size());
        return false;
    }
    output = input;
    ret = modelScoresToOutputTable(modelScores, OUTPUT_SCORE_NAME, output);
    if (!ret) {
        SQL_LOG(ERROR, "transfer model scores to output table failed");
        return false;
    }
    return true;
}

void GraphSearchTvfFunc::prepareGraphRequest(
        const std::string &bizName,
        const Tensor &qinfoTensor,
        const Tensor &pkTensor,
        const Tensor &contextTensor,
        suez::turing::GraphRequest &graphRequest) {

    graphRequest.set_bizname(bizName);
    suez::turing::GraphInfo &graphInfo = *graphRequest.mutable_graphinfo();

    suez::turing::NamedTensorProto &qinfoTensorProto = *graphInfo.add_inputs();
    qinfoTensorProto.set_name(INPUT_QINFO_NAME);
    qinfoTensor.AsProtoField(qinfoTensorProto.mutable_tensor());

    suez::turing::NamedTensorProto &pkTensorProto = *graphInfo.add_inputs();
    pkTensorProto.set_name(INPUT_PK_NAME);
    pkTensor.AsProtoField(pkTensorProto.mutable_tensor());

    suez::turing::NamedTensorProto &contextTensorProto = *graphInfo.add_inputs();
    contextTensorProto.set_name(INPUT_CONTEXT_NAME);
    contextTensor.AsProtoField(contextTensorProto.mutable_tensor());

    *graphInfo.add_fetches() = OUTPUT_FETCHES_NAME;
    *graphInfo.add_targets() = OUTPUT_TARGETS_NAME;
}


bool GraphSearchTvfFunc::getModelScoresFromGraphResponse(
        const suez::turing::GraphResponse &graphResponse,
        vector<float> &modelScoresVec) {
    if (graphResponse.outputs_size() != 1) {
        SQL_LOG(ERROR, "invalid graph response outputs size[%d]",
                (int32_t)graphResponse.outputs_size());
        return false;
    }
    const tensorflow::TensorProto &tensorProto =  graphResponse.outputs(0).tensor();
    if (tensorProto.dtype() != DT_FLOAT) {
        SQL_LOG(ERROR, "invalid tensor proto type[%d]",
                (int32_t)tensorProto.dtype());
        return false;
    }
    auto modelScoresTensor = Tensor(DT_FLOAT, TensorShape({}));
    if (!modelScoresTensor.FromProto(tensorProto)) {
        SQL_LOG(ERROR, "decode model scores tensor from proto failed");
        return false;
    }
    const float *modelScores =  modelScoresTensor.base<float>();
    const size_t modelScoresCnt = modelScoresTensor.NumElements();
    if (modelScores) {
        modelScoresVec.resize(modelScoresCnt);
        for (size_t i = 0; i < modelScoresCnt; i++) {
            modelScoresVec[i] = modelScores[i];
        }
    }

    return true;
}

bool GraphSearchTvfFunc::prepareQInfoKvPairsMap(
        const string &qinfoPairsStr,
        map<string, string> &qinfoPairsMap) {
    size_t start = 0;
    while (start < qinfoPairsStr.length()) {
        string key = getNextTerm(qinfoPairsStr,
                KVPAIR_CLAUSE_KV_SEPERATOR, start);
        if (start >= qinfoPairsStr.length()) {
            if (!key.empty()) {
                SQL_LOG(ERROR,
                        "Parse qinfo kvpairs failed, Invalid kvpairs: [%s]",
                        qinfoPairsStr.c_str());
                return false;
            }
            else {
                break;
            }
        }
        string value = getNextTerm(qinfoPairsStr,
                KVPAIR_CLAUSE_KV_PAIR_SEPERATOR, start);
        qinfoPairsMap.insert(make_pair(key, value));
    }

    return true;
}

void GraphSearchTvfFunc::prepareQInfoTensor(
        const map<string, string> &qinfoPairsMap,
        Tensor &qinfoTensor) {
    qinfoTensor = Tensor(DT_VARIANT, TensorShape({}));
    QInfoFieldVariant qinfoVariant;
    qinfoVariant.copyFromKvMap(qinfoPairsMap);
    qinfoTensor.scalar<Variant>()() = qinfoVariant;
}

void GraphSearchTvfFunc::preparePKTensor(
        const std::vector<int64_t> &pkVec,
        tensorflow::Tensor &pkTensor) {
    pkTensor = Tensor(DT_INT64, TensorShape({(int64_t)pkVec.size()}));
    auto pks = pkTensor.flat<int64>();
    for (size_t i = 0; i < pkVec.size(); i++) {
        pks(i) = pkVec[i];
    }
}

void GraphSearchTvfFunc::prepareContextTensor(Tensor &contextTensor) {
    contextTensor = Tensor(DT_VARIANT, TensorShape({}));
    RawTensorVariant contextVariant;
    contextTensor.scalar<Variant>()() = contextVariant;
}

string GraphSearchTvfFunc::getNextTerm(
        const string &inputStr, char sep, size_t &start) {
    string ret;
    ret.reserve(64);
    while (start < inputStr.length()) {
        char c = inputStr[start];
        ++start;
        if (c == sep) {
            break;
        } else if (c == '\\') {
            if (start < inputStr.length()) {
                ret += inputStr[start];
                ++start;
            }
            continue;
        }
        ret += c;
    }
    StringUtil::trim(ret);
    return ret;
}

GraphSearchTvfFuncCreator::GraphSearchTvfFuncCreator()
    : TvfFuncCreator(graphSearchTvfDef, graphSearchTvfInfo)
{}

GraphSearchTvfFuncCreator::~GraphSearchTvfFuncCreator() {
}

TvfFunc *GraphSearchTvfFuncCreator::createFunction(
        const HA3_NS(config)::SqlTvfProfileInfo &info) {
    return new GraphSearchTvfFunc();
}

END_HA3_NAMESPACE(builtin);
