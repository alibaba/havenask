#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/ops/tvf/TvfFunc.h>
#include <ha3/sql/ops/tvf/TvfFuncCreator.h>
#include <suez/turing/common/LocalSearchService.h>
#include <tensorflow/core/framework/tensor.h>
BEGIN_HA3_NAMESPACE(sql);

class GraphSearchTvfFunc : public OnePassTvfFunc
{
public:
    GraphSearchTvfFunc();
    ~GraphSearchTvfFunc();
    GraphSearchTvfFunc(const GraphSearchTvfFunc &) = delete;
    GraphSearchTvfFunc& operator=(const GraphSearchTvfFunc &) = delete;
private:
    bool init(TvfFuncInitContext &context) override;
    bool doCompute(const TablePtr &input, TablePtr &output);
private:
    bool extractPkVec(const std::string &pkColumnName, const TablePtr &input,
                      std::vector<int64_t> &pkVec);
    bool modelScoresToOutputTable(
            const std::vector<float> &modelScores,
            const std::string &outputScoreColumnName,
            TablePtr &output);
    void prepareGraphRequest(
            const std::string &bizName,
            const tensorflow::Tensor &qinfoTensor,
            const tensorflow::Tensor &pkTensor,
            const tensorflow::Tensor &contextTensor,
            suez::turing::GraphRequest &graphRequest);
    bool getModelScoresFromGraphResponse(
            const suez::turing::GraphResponse &graphResponse,
            std::vector<float> &modelScoresVec);
    bool prepareQInfoKvPairsMap(const std::string &qinfoPairsStr,
                               std::map<std::string, std::string> &qinfoPairsMap);
    void prepareQInfoTensor(
            const std::map<std::string, std::string> &qinfoPairsMap,
            tensorflow::Tensor &qinfoTensor);
    void preparePKTensor(
            const std::vector<int64_t> &pkVec,
            tensorflow::Tensor &pkTensor);
    void prepareContextTensor(tensorflow::Tensor &contextTensor);
private:
    std::string getNextTerm(const std::string &inputStr, char sep, size_t &start);
private:
    std::string _bizName;
    std::string _pkColumnName;
    std::map<std::string, std::string> _qinfoPairsMap;
    autil::mem_pool::Pool *_queryPool;
    suez::turing::LocalSearchServicePtr _localSearchService;
private:
    static const std::string INPUT_QINFO_NAME;
    static const std::string INPUT_PK_NAME;
    static const std::string INPUT_CONTEXT_NAME;
    static const std::string OUTPUT_FETCHES_NAME;
    static const std::string OUTPUT_TARGETS_NAME;
    static const std::string OUTPUT_SCORE_NAME;
};

HA3_TYPEDEF_PTR(GraphSearchTvfFunc);

class GraphSearchTvfFuncCreator : public TvfFuncCreator
{
public:
    GraphSearchTvfFuncCreator();
    ~GraphSearchTvfFuncCreator();
private:
    GraphSearchTvfFuncCreator(const GraphSearchTvfFuncCreator &) = delete;
    GraphSearchTvfFuncCreator& operator=(const GraphSearchTvfFuncCreator &) = delete;
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override;
};
HA3_TYPEDEF_PTR(GraphSearchTvfFuncCreator);





END_HA3_NAMESPACE(sql);
