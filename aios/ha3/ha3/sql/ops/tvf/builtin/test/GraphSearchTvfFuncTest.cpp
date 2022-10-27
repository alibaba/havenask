#include <ha3/sql/ops/test/OpTestBase.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/sql/ops/tvf/builtin/GraphSearchTvfFunc.h"
#include <ha3/sql/ops/tvf/TvfLocalSearchResource.h>
#include <ha3/sql/data/TableUtil.h>
#include <basic_ops/variant/QInfoFieldVariant.h>
#include <suez/turing/search/GraphServiceImpl.h>
using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace tensorflow;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(sql);

class RunGraphService: public GraphServiceImpl {
public:
    RunGraphService() : isSetScore(true) {}
public:
    void runGraph(::google::protobuf::RpcController *controller, const GraphRequest *request,
                  GraphResponse *response, ::google::protobuf::Closure *done) {
        *response->mutable_errorinfo() = errorInfo;
        if (isSetScore) {
            Tensor scoreTensor = Tensor(DT_FLOAT,
                    TensorShape({(int64_t)modelScores.size()}));
            auto scores = scoreTensor.flat<float>();
            for (size_t i = 0; i < modelScores.size(); i ++) {
                scores(i) = modelScores[i];
            }
            suez::turing::NamedTensorProto *namedTensor = response->add_outputs();
            namedTensor->set_name("modelscore");
            scoreTensor.AsProtoField(namedTensor->mutable_tensor());
        }
        done->Run();
    }
public:
    bool isSetScore;
    ErrorInfo errorInfo;
    vector<float> modelScores;
};

class GraphSearchTvfFuncTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
    TablePtr  prepareInputTable(vector<int32_t> value);
public:
    MatchDocAllocatorPtr _allocator;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(builtin, GraphSearchTvfFuncTest);

void GraphSearchTvfFuncTest::setUp() {
}

void GraphSearchTvfFuncTest::tearDown() {
}

TablePtr GraphSearchTvfFuncTest::prepareInputTable(vector<int32_t> value) {
    MatchDocAllocatorPtr allocator(new matchdoc::MatchDocAllocator(_poolPtr));
    vector<MatchDoc> docs = allocator->batchAllocate(value);
    extendMatchDocAllocator<int32_t>(allocator, docs, "id", value);
    TablePtr table;
    table.reset(new Table(docs, allocator));
    return table;
}

TEST_F(GraphSearchTvfFuncTest, testInit) {
    {
        TvfFuncInitContext context;
        GraphSearchTvfFunc tvfFunc;
        string bizName = "test_biz";
        string pkColumnName = "pk_column";
        string kvPairStr = "key1:value1,key2:value2";
        context.params.push_back(bizName);
        context.params.push_back(pkColumnName);
        context.params.push_back(kvPairStr);

        shared_ptr<GraphServiceImpl> runGraphService(new RunGraphService);
        LocalSearchServicePtr localSearchService(
                new LocalSearchService(runGraphService.get()));
        TvfResourceContainer resourceContainer;
        resourceContainer.put(new TvfLocalSearchResource(localSearchService));
        context.tvfResourceContainer = &resourceContainer;
        ASSERT_TRUE(tvfFunc.init(context));
        ASSERT_EQ(bizName, tvfFunc._bizName);
        ASSERT_EQ(pkColumnName, tvfFunc._pkColumnName);
        ASSERT_EQ(2u, tvfFunc._qinfoPairsMap.size());
        ASSERT_EQ(string("value1"), tvfFunc._qinfoPairsMap["key1"]);
        ASSERT_EQ(string("value2"), tvfFunc._qinfoPairsMap["key2"]);
    }

    {
        TvfFuncInitContext context;
        GraphSearchTvfFunc tvfFunc;
        string bizName = "test_biz";
        string pkColumnName = "pk_column";
        context.params.push_back(bizName);
        context.params.push_back(pkColumnName);

        ASSERT_FALSE(tvfFunc.init(context));
    }

    {
        TvfFuncInitContext context;
        GraphSearchTvfFunc tvfFunc;
        string bizName = "test_biz";
        string pkColumnName = "pk_column";
        string kvPairStr = "key1:value1,key2";
        context.params.push_back(bizName);
        context.params.push_back(pkColumnName);
        context.params.push_back(kvPairStr);

        ASSERT_FALSE(tvfFunc.init(context));
    }

    {
        TvfFuncInitContext context;
        GraphSearchTvfFunc tvfFunc;
        string bizName = "test_biz";
        string pkColumnName = "pk_column";
        string kvPairStr = "key1:value1,key2:value2";
        context.params.push_back(bizName);
        context.params.push_back(pkColumnName);
        context.params.push_back(kvPairStr);

        TvfResourceContainer resourceContainer;
        context.tvfResourceContainer = &resourceContainer;
        ASSERT_FALSE(tvfFunc.init(context));
    }

    {
        TvfFuncInitContext context;
        GraphSearchTvfFunc tvfFunc;
        string bizName = "test_biz";
        string pkColumnName = "pk_column";
        string kvPairStr = "key1:value1,key2:value2";
        context.params.push_back(bizName);
        context.params.push_back(pkColumnName);
        context.params.push_back(kvPairStr);

        shared_ptr<GraphServiceImpl> runGraphService(new RunGraphService);
        LocalSearchServicePtr localSearchService;
        TvfResourceContainer resourceContainer;
        resourceContainer.put(new TvfLocalSearchResource(localSearchService));
        context.tvfResourceContainer = &resourceContainer;
        ASSERT_FALSE(tvfFunc.init(context));
    }
}


TEST_F(GraphSearchTvfFuncTest, testPrepareQInfoKvPairsMap) {
    GraphSearchTvfFunc graphFunc;
    {
        //normal
        const string qinfoPairStr = "key1:value1,key2:value2,key3:value3";
        map<string, string> qinfoPairsMap;
        bool res = graphFunc.prepareQInfoKvPairsMap(qinfoPairStr, qinfoPairsMap);
        ASSERT_TRUE(res);
        ASSERT_EQ(3u, qinfoPairsMap.size());
        ASSERT_EQ(string("value1"), qinfoPairsMap["key1"]);
        ASSERT_EQ(string("value2"), qinfoPairsMap["key2"]);
        ASSERT_EQ(string("value3"), qinfoPairsMap["key3"]);
    }
    {
        //invalid kvpairStr
        const string qinfoPairStr = "key1:value1,key2";
        map<string, string> qinfoPairsMap;
        bool res = graphFunc.prepareQInfoKvPairsMap(qinfoPairStr, qinfoPairsMap);
        ASSERT_TRUE(!res);
        ASSERT_EQ(1u, qinfoPairsMap.size());
        ASSERT_EQ(string("value1"), qinfoPairsMap["key1"]);
    }
    {
        //invalid kvpairStr
        const string qinfoPairStr = "key1";
        map<string, string> qinfoPairsMap;
        bool res = graphFunc.prepareQInfoKvPairsMap(qinfoPairStr, qinfoPairsMap);
        ASSERT_TRUE(!res);
        ASSERT_EQ(0u, qinfoPairsMap.size());
    }
    {
        //key\value with comma
        const string qinfoPairStr = "key1:value1,key2:value2_1\\,value2_2,key3_1\\,key3_2:value3";
        map<string, string> qinfoPairsMap;
        bool res = graphFunc.prepareQInfoKvPairsMap(qinfoPairStr, qinfoPairsMap);
        ASSERT_TRUE(res);
        ASSERT_EQ(3u, qinfoPairsMap.size());
        ASSERT_EQ(string("value1"), qinfoPairsMap["key1"]);
        ASSERT_EQ(string("value2_1,value2_2"), qinfoPairsMap["key2"]);
        ASSERT_EQ(string("value3"), qinfoPairsMap["key3_1,key3_2"]);
    }

}

TEST_F(GraphSearchTvfFuncTest, testPrepareQInfoTensor) {
    GraphSearchTvfFunc graphFunc;
    {
        //normal
        map<string, string> qinfoPairsMap;
        qinfoPairsMap["fg_user"] = "user_sex:16;user_power:17#12";
        Tensor qinfoTensor;
        graphFunc.prepareQInfoTensor(qinfoPairsMap, qinfoTensor);
        QInfoFieldVariant *qinfoVariant =
            qinfoTensor.scalar<Variant>()().get<QInfoFieldVariant>();
        ASSERT_TRUE(qinfoVariant != nullptr);
        string expectStr = R"json({"user:user_sex":["16"],"user:user_power":["17","12"]})json";
        ASSERT_EQ(expectStr, qinfoVariant->toJsonString());
    }
}

TEST_F(GraphSearchTvfFuncTest, testPreparePKTensor) {
    GraphSearchTvfFunc graphFunc;
    {
        //normal
        vector<int64_t> pkVec = {123, 456};
        Tensor pkTensor;
        graphFunc.preparePKTensor(pkVec, pkTensor);
        auto pkCount = pkTensor.dim_size(0);
        auto pks = pkTensor.flat<int64>();
        ASSERT_EQ(2u, pkCount);
        for (size_t i = 0; i < pkVec.size(); i++) {
            ASSERT_EQ(pkVec[i], pks(i));
        }

    }
}

TEST_F(GraphSearchTvfFuncTest, testPrepareGraphRequest) {
    GraphSearchTvfFunc graphFunc;
    {
        //normal
        string bizName = "default.biz";
        Tensor qinfoTensor;
        Tensor pkTensor;
        Tensor contextTensor;
        GraphRequest graphRequest;
        map<string, string> qinfoPairsMap;
        qinfoPairsMap["fg_user"] = "user_sex:16;user_power:17#12";
        vector<int64_t> pkVec = {123, 456};
        graphFunc.prepareQInfoTensor(qinfoPairsMap, qinfoTensor);
        graphFunc.preparePKTensor(pkVec, pkTensor);
        graphFunc.prepareGraphRequest(bizName, qinfoTensor, pkTensor, contextTensor,
                                      graphRequest);
        ASSERT_EQ(bizName, graphRequest.bizname());
        const auto &graphInfo = graphRequest.graphinfo();
        ASSERT_EQ(1, graphInfo.fetches_size());
        ASSERT_EQ(string("modelscore"), graphInfo.fetches(0));
        ASSERT_EQ(1, graphInfo.targets_size());
        ASSERT_EQ(string("modelscore"), graphInfo.targets(0));

        ASSERT_EQ(3, graphInfo.inputs_size());
        ASSERT_EQ(string("qinfo"), graphInfo.inputs(0).name());
        const auto &qinfoProto = graphInfo.inputs(0).tensor();
        ASSERT_EQ(DT_VARIANT, qinfoProto.dtype());
        auto resQinfoTensor = Tensor(DT_VARIANT, TensorShape({}));
        ASSERT_TRUE(resQinfoTensor.FromProto(qinfoProto));
        QInfoFieldVariant *qinfoVariant =
            resQinfoTensor.scalar<Variant>()().get<QInfoFieldVariant>();
        ASSERT_TRUE(qinfoVariant != nullptr);
        string expectStr = R"json({"user:user_sex":["16"],"user:user_power":["17","12"]})json";
        ASSERT_EQ(expectStr, qinfoVariant->toJsonString());
        ASSERT_EQ(string("pk"), graphInfo.inputs(1).name());
        const auto &pkProto = graphInfo.inputs(1).tensor();
        ASSERT_EQ(DT_INT64, pkProto.dtype());
        auto resPkTensor = Tensor(DT_INT64, TensorShape({}));
        ASSERT_TRUE(resPkTensor.FromProto(pkProto));
        auto pkCount = resPkTensor.dim_size(0);
        auto pks = resPkTensor.flat<int64>();
        ASSERT_EQ(2u, pkCount);
        for (size_t i = 0; i < pkVec.size(); i++) {
            ASSERT_EQ(pkVec[i], pks(i));
        }
        ASSERT_EQ(string("context"), graphInfo.inputs(2).name());

    }
}

TEST_F(GraphSearchTvfFuncTest, testGetModelScoresFromGraphResponse) {
    GraphSearchTvfFunc graphFunc;
    {
        //normal
        GraphResponse graphResponse;
        vector<float> modelScoresVec;
        Tensor scoreTensor = Tensor(DT_FLOAT, TensorShape({(int64_t)5}));
        auto scores = scoreTensor.flat<float>();
        for (size_t i = 0; i < 5; i ++) {
            scores(i) = i * 5.0;
        }
        suez::turing::NamedTensorProto *namedTensor = graphResponse.add_outputs();
        namedTensor->set_name("modelscore");
        scoreTensor.AsProtoField(namedTensor->mutable_tensor());
        ASSERT_TRUE(graphFunc.getModelScoresFromGraphResponse(graphResponse, modelScoresVec));
        ASSERT_EQ(5u, modelScoresVec.size());
        for (size_t i = 0; i < 5; i ++) {
            ASSERT_FLOAT_EQ(i * 5.0, modelScoresVec[i]);
        }
    }

    {
        //size = 0
        GraphResponse graphResponse;
        vector<float> modelScoresVec;

        ASSERT_FALSE(graphFunc.getModelScoresFromGraphResponse(graphResponse, modelScoresVec));
    }

    {
        //size = 2
        GraphResponse graphResponse;
        vector<float> modelScoresVec;
        Tensor scoreTensor = Tensor(DT_FLOAT, TensorShape({(int64_t)5}));
        auto scores = scoreTensor.flat<float>();
        for (size_t i = 0; i < 5; i ++) {
            scores(i) = i * 5.0;
        }
        suez::turing::NamedTensorProto *namedTensor1 = graphResponse.add_outputs();
        namedTensor1->set_name("modelscore1");
        scoreTensor.AsProtoField(namedTensor1->mutable_tensor());

        suez::turing::NamedTensorProto *namedTensor2 = graphResponse.add_outputs();
        namedTensor2->set_name("modelscore2");
        scoreTensor.AsProtoField(namedTensor2->mutable_tensor());

        ASSERT_FALSE(graphFunc.getModelScoresFromGraphResponse(graphResponse, modelScoresVec));

    }

    {
        //type != float
        GraphResponse graphResponse;
        vector<float> modelScoresVec;
        Tensor scoreTensor = Tensor(DT_INT32, TensorShape({(int64_t)5}));
        auto scores = scoreTensor.flat<int32>();
        for (size_t i = 0; i < 5; i ++) {
            scores(i) = (int32_t)i;
        }

        suez::turing::NamedTensorProto *namedTensor = graphResponse.add_outputs();
        namedTensor->set_name("modelscore");
        scoreTensor.AsProtoField(namedTensor->mutable_tensor());

        ASSERT_FALSE(graphFunc.getModelScoresFromGraphResponse(graphResponse, modelScoresVec));

    }

}

TEST_F(GraphSearchTvfFuncTest, testExtractPkVec) {
    {
        TablePtr inputTable;
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 5));
        vector<int64_t> expectedPkVec = {1, 2, 3, 4, 5};
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(
                        _allocator, docs, "pk_column", expectedPkVec));

        inputTable = getTable(createTable(_allocator, docs));

        GraphSearchTvfFunc graphFunc;
        vector<int64_t> pkVec;
        ASSERT_TRUE(graphFunc.extractPkVec("pk_column", inputTable, pkVec));
        ASSERT_EQ(expectedPkVec.size(), pkVec.size());
        for (size_t i = 0; i < expectedPkVec.size(); i++) {
            ASSERT_EQ(expectedPkVec[i], pkVec[i]);
        }
    }

    {
        TablePtr inputTable;
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 0));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(
                        _allocator, docs, "pk_column", {}));

        inputTable = getTable(createTable(_allocator, docs));

        GraphSearchTvfFunc graphFunc;
        vector<int64_t> pkVec;
        ASSERT_TRUE(graphFunc.extractPkVec("pk_column", inputTable, pkVec));
        ASSERT_EQ(0u, pkVec.size());
    }

    {
        TablePtr inputTable;
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 5));
        vector<int64_t> expectedPkVec = {1, 2, 3, 4, 5};
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(
                        _allocator, docs, "pk_column", expectedPkVec));

        inputTable = getTable(createTable(_allocator, docs));

        GraphSearchTvfFunc graphFunc;
        vector<int64_t> pkVec;
        ASSERT_FALSE(graphFunc.extractPkVec("no_pk_column", inputTable, pkVec));
    }

    {
        TablePtr inputTable;
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 5));
        vector<int32_t> expectedPkVec = {1, 2, 3, 4, 5};
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(
                        _allocator, docs, "pk_column", expectedPkVec));

        inputTable = getTable(createTable(_allocator, docs));

        GraphSearchTvfFunc graphFunc;
        vector<int64_t> pkVec;
        ASSERT_FALSE(graphFunc.extractPkVec("pk_column", inputTable, pkVec));
    }

}

TEST_F(GraphSearchTvfFuncTest, testModelScoresToOutputTable) {
    {
        TablePtr inputTable;
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 5));
        vector<int64_t> pkVec = {1, 2, 3, 4, 5};
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(
                        _allocator, docs, "pk_column", pkVec));

        inputTable = getTable(createTable(_allocator, docs));
        auto outputTable = inputTable;
        GraphSearchTvfFunc graphFunc;
        vector<float> modelScores = {1.0, 2.0, 3.0, 4.0, 5.0};
        ASSERT_TRUE(graphFunc.modelScoresToOutputTable(modelScores,
                        "score_column", outputTable));

        ColumnData<float> *scoreColumn =
            TableUtil::getColumnData<float>(outputTable, "score_column");
        ASSERT_TRUE(scoreColumn != nullptr);
        for (size_t i = 0; i < modelScores.size(); i++) {
            ASSERT_FLOAT_EQ(modelScores[i], scoreColumn->get(i));
        }
    }

    {
        TablePtr inputTable;
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 5));
        vector<int64_t> pkVec = {1, 2, 3, 4, 5};
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(
                        _allocator, docs, "pk_column", pkVec));

        inputTable = getTable(createTable(_allocator, docs));
        auto outputTable = inputTable;
        GraphSearchTvfFunc graphFunc;
        vector<float> modelScores = {1.0, 2.0, 3.0, 4.0, 5.0};
        ASSERT_FALSE(graphFunc.modelScoresToOutputTable(modelScores,
                        "pk_column", outputTable));
    }

    {
        TablePtr inputTable;
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 5));
        vector<int64_t> pkVec = {1, 2, 3, 4, 5};
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(
                        _allocator, docs, "pk_column", pkVec));

        inputTable = getTable(createTable(_allocator, docs));
        auto outputTable = inputTable;
        GraphSearchTvfFunc graphFunc;
        vector<float> modelScores = {1.0, 2.0, 3.0, 4.0};
        ASSERT_FALSE(graphFunc.modelScoresToOutputTable(modelScores,
                        "score_column", outputTable));
    }
}

TEST_F(GraphSearchTvfFuncTest, testCompute) {
    {
        // normal
        shared_ptr<GraphServiceImpl> runGraphService(new RunGraphService);
        vector<float> expectedModelScores = {5.0, 4.0, 3.0, 2.0, 1.0};
        ((RunGraphService *)runGraphService.get())->modelScores =
            expectedModelScores;

        LocalSearchServicePtr localSearchService(
                new LocalSearchService(runGraphService.get()));
        GraphSearchTvfFunc tvfFunc;
        tvfFunc._bizName = "test_biz";
        tvfFunc._pkColumnName = "pk_column";
        tvfFunc._localSearchService = localSearchService;
        TablePtr inputTable;
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 5));
        vector<int64_t> pkVec = {1, 2, 3, 4, 5};
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(
                        _allocator, docs, "pk_column", pkVec));

        inputTable = getTable(createTable(_allocator, docs));
        TablePtr outputTable;
        ASSERT_TRUE(tvfFunc.doCompute(inputTable, outputTable));
        ASSERT_TRUE(outputTable != nullptr);
        ColumnData<float> *scoreColumn =
            TableUtil::getColumnData<float>(outputTable,
                    GraphSearchTvfFunc::OUTPUT_SCORE_NAME);
        ASSERT_TRUE(scoreColumn != nullptr);
        for (size_t i = 0; i < expectedModelScores.size(); i++) {
            ASSERT_FLOAT_EQ(expectedModelScores[i], scoreColumn->get(i));
        }
    }

    {
        // extract pv failed
        shared_ptr<GraphServiceImpl> runGraphService(new RunGraphService);
        vector<float> expectedModelScores = {5.0, 4.0, 3.0, 2.0, 1.0};
        ((RunGraphService *)runGraphService.get())->modelScores =
            expectedModelScores;

        LocalSearchServicePtr localSearchService(
                new LocalSearchService(runGraphService.get()));
        GraphSearchTvfFunc tvfFunc;
        tvfFunc._bizName = "test_biz";
        tvfFunc._pkColumnName = "pk_column_not_exist";
        tvfFunc._localSearchService = localSearchService;
        TablePtr inputTable;
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 5));
        vector<int64_t> pkVec = {1, 2, 3, 4, 5};
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(
                        _allocator, docs, "pk_column", pkVec));

        inputTable = getTable(createTable(_allocator, docs));
        TablePtr outputTable;
        ASSERT_FALSE(tvfFunc.doCompute(inputTable, outputTable));
    }
    {
        // error code is not zero
        shared_ptr<GraphServiceImpl> runGraphService(new RunGraphService);
        vector<float> expectedModelScores = {5.0, 4.0, 3.0, 2.0, 1.0};
        ((RunGraphService *)runGraphService.get())->modelScores =
            expectedModelScores;

        ((RunGraphService *)runGraphService.get())->errorInfo.set_errorcode(
                RS_ERROR_PARSE_REQUEST);

        LocalSearchServicePtr localSearchService(
                new LocalSearchService(runGraphService.get()));
        GraphSearchTvfFunc tvfFunc;
        tvfFunc._bizName = "test_biz";
        tvfFunc._pkColumnName = "pk_column";
        tvfFunc._localSearchService = localSearchService;
        TablePtr inputTable;
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 5));
        vector<int64_t> pkVec = {1, 2, 3, 4, 5};
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(
                        _allocator, docs, "pk_column", pkVec));

        inputTable = getTable(createTable(_allocator, docs));
        TablePtr outputTable;
        ASSERT_FALSE(tvfFunc.doCompute(inputTable, outputTable));
    }
    {
        // get model scores from graph response failed
        shared_ptr<GraphServiceImpl> runGraphService(new RunGraphService);
        vector<float> expectedModelScores = {5.0, 4.0, 3.0, 2.0, 1.0};
        ((RunGraphService *)runGraphService.get())->modelScores =
            expectedModelScores;
        ((RunGraphService *)runGraphService.get())->isSetScore = false;

        LocalSearchServicePtr localSearchService(
                new LocalSearchService(runGraphService.get()));
        GraphSearchTvfFunc tvfFunc;
        tvfFunc._bizName = "test_biz";
        tvfFunc._pkColumnName = "pk_column";
        tvfFunc._localSearchService = localSearchService;
        TablePtr inputTable;
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 5));
        vector<int64_t> pkVec = {1, 2, 3, 4, 5};
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(
                        _allocator, docs, "pk_column", pkVec));

        inputTable = getTable(createTable(_allocator, docs));
        TablePtr outputTable;
        ASSERT_FALSE(tvfFunc.doCompute(inputTable, outputTable));
    }
    {
        // pk vec size is not equal to model scores size
        shared_ptr<GraphServiceImpl> runGraphService(new RunGraphService);
        vector<float> expectedModelScores = {5.0, 4.0, 3.0, 2.0};
        ((RunGraphService *)runGraphService.get())->modelScores =
            expectedModelScores;

        LocalSearchServicePtr localSearchService(
                new LocalSearchService(runGraphService.get()));
        GraphSearchTvfFunc tvfFunc;
        tvfFunc._bizName = "test_biz";
        tvfFunc._pkColumnName = "pk_column";
        tvfFunc._localSearchService = localSearchService;
        TablePtr inputTable;
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 5));
        vector<int64_t> pkVec = {1, 2, 3, 4, 5};
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(
                        _allocator, docs, "pk_column", pkVec));

        inputTable = getTable(createTable(_allocator, docs));
        TablePtr outputTable;
        ASSERT_FALSE(tvfFunc.doCompute(inputTable, outputTable));
    }
    {
        // model scores to output table failed
        shared_ptr<GraphServiceImpl> runGraphService(new RunGraphService);
        vector<float> expectedModelScores = {5.0, 4.0, 3.0, 2.0, 1.0};
        ((RunGraphService *)runGraphService.get())->modelScores =
            expectedModelScores;

        LocalSearchServicePtr localSearchService(
                new LocalSearchService(runGraphService.get()));
        GraphSearchTvfFunc tvfFunc;
        tvfFunc._bizName = "test_biz";
        tvfFunc._pkColumnName = GraphSearchTvfFunc::OUTPUT_SCORE_NAME;
        tvfFunc._localSearchService = localSearchService;
        TablePtr inputTable;
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 5));
        vector<int64_t> pkVec = {1, 2, 3, 4, 5};
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(
                        _allocator, docs, GraphSearchTvfFunc::OUTPUT_SCORE_NAME,
                        pkVec));

        inputTable = getTable(createTable(_allocator, docs));
        TablePtr outputTable;
        ASSERT_FALSE(tvfFunc.doCompute(inputTable, outputTable));
    }
}

END_HA3_NAMESPACE();
