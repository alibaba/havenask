#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/graph/node_builder.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <tensorflow/core/public/session.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>
#include <ha3/turing/ops/Ha3SummaryOp.h>
#include <ha3/service/SearcherResource.h>
#include <ha3/summary/SummaryProfileManager.h>
#include <ha3/summary/SummaryProfileManagerCreator.h>
#include <ha3/search/test/FakeSummaryExtractor.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <suez/turing/expression/function/FunctionInterfaceCreator.h>
#include <ha3/search/test/FakeSummaryExtractor.h>
#include <ha3/turing/ops/Ha3ExtraSummaryOp.h>

using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;

IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(summary);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(func_expression);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(monitor);

BEGIN_HA3_NAMESPACE(turing);


class Ha3ExtraSummaryOpTest : public Ha3OpTestBase
{
public:
    void SetUp() override {
        OpTestBase::SetUp();
        removeIndex();

        _summaryOp = NULL;
        prepareRequest();
        prepareIndex();
        _resourceReaderPtr.reset(new ResourceReader("."));

        _funcCreator.reset(new FunctionInterfaceCreator);
        _hitSummarySchema.reset(new HitSummarySchema(_tableInfoPtr.get()));
        _hitSummarySchemaPool.reset(new HitSummarySchemaPool(_hitSummarySchema));
        initSummaryProfileManager();

        // init searcherResource
        _searcherResource.reset(new HA3_NS(service)::SearcherResource);
        _searcherResource->setIndexPartitionWrapper(_indexPartPtr);
        _searcherResource->setHitSummarySchema(_hitSummarySchema);
        _searcherResource->setHitSummarySchemaPool(_hitSummarySchemaPool);
        _searcherResource->setTableInfo(_tableInfoPtr);
        _searcherResource->setSummaryProfileManager(_summaryProfileManagerPtr);

        ClusterConfigInfo clusterConfigInfo;
        clusterConfigInfo.setTableName("summary");
        _searcherResource->setClusterConfig(clusterConfigInfo);
        // init SessionMetricsCollector
        _collector.reset(new SessionMetricsCollector(NULL));
    }

    void TearDown() override {
        _fakeSummaryExtractor = NULL;
        _searcherResource.reset();
        _hitSummarySchema.reset();
        _hitSummarySchemaPool.reset();
        _tableInfoPtr.reset();
        _request.reset();
        _summaryOp = NULL;
        removeIndex();
        OpTestBase::TearDown();
    }

private:
    void createIndexPartition(string tableName, string docString) {
        // schemaName, fields, indexs, attributes, summarys, truncateProfile
        auto schema = IndexlibPartitionCreator::CreateSchema(
            tableName, "pk:int32;attr:int32;summary:string;",
            "pk:primarykey64:pk:number_hash", "attr", "pk;attr;summary", "");
        IndexPartitionOptions options;
        options.GetBuildConfig(false).enablePackageFile = false;
        options.GetOnlineConfig().enableAsyncCleanResource = false;
        auto indexPartition = IndexlibPartitionCreator::CreateIndexPartition(
            schema, _rootPath + "/" + tableName, docString, options);
        string schemaName = indexPartition->GetSchema()->GetSchemaName();
        _indexPartitionMap[schemaName] = indexPartition;
        _sessionResource->dependencyTableInfoMap[tableName] =
            TableInfoConfigurator::createFromSchema(schema);
    }

    void prepareRequest() {
        _request.reset(new common::Request());
        _request->setDocIdClause(new common::DocIdClause);
        _request->setConfigClause(new common::ConfigClause);
        _request->getConfigClause()->setFetchSummaryType(BY_PK);
        _request->setQueryClause(new common::QueryClause);
    }

    void prepareIndex() {
        string hotDocStrs = "cmd=add,pk=0,attr=0,summary=s0;"
                            "cmd=add,pk=2,attr=2,summary=s2;";

        string coldDocStrs = "cmd=add,pk=1,attr=1,summary=s1;"
                             "cmd=add,pk=3,attr=3,summary=s3;"
                             "cmd=add,pk=4,attr=4,summary=s4;";
        createIndexPartition("summary", hotDocStrs);
        createIndexPartition("summary_extra", coldDocStrs);
        _indexApp.reset(new IndexApplication);
        _indexApp->Init(_indexPartitionMap, JoinRelationMap());
        _snapshotPtr = _indexApp->CreateSnapshot();
        _tableInfoPtr = TableInfoConfigurator::createFromIndexApp("summary", _indexApp);
        _indexPartPtr.reset(new IndexPartitionWrapper);
        _indexPartPtr->init(_indexPartitionMap["summary"]);
    }

    void removeIndex() {
        FileUtil::removeLocalDir(_rootPath + "/summary", true);
        FileUtil::removeLocalDir(_rootPath + "/summary_extra", true);
    }

    void initSummaryProfileManager() {
        SummaryProfileManagerCreator creator(_resourceReaderPtr, _hitSummarySchema);
        string configStr = FileUtil::readFile(TEST_DATA_PATH"/testSummaryOp/summary_config");
        _summaryProfileManagerPtr = creator.createFromString(configStr);
        _fakeSummaryExtractor = new FakeSummaryExtractor;
        SummaryProfileInfo summaryProfileInfo;
        summaryProfileInfo._summaryProfileName = "daogou";
        SummaryProfile *summaryProfile = new SummaryProfile(summaryProfileInfo,
                _hitSummarySchema.get());
        summaryProfile->resetSummaryExtractor(_fakeSummaryExtractor);
        _summaryProfileManagerPtr->addSummaryProfile(summaryProfile);
    }

    void prepareSummaryOp() {
        Graph g(OpRegistry::Global());
        Node* placeholder;
        TF_ASSERT_OK(
            NodeBuilder("ha3_request", "Placeholder")
            .Attr("dtype", DT_VARIANT)
            .Finalize(&g, &placeholder));

        Node *summary;
        TF_ASSERT_OK(
            NodeBuilder("summary_op", "Ha3SummaryOp")
            .Input(placeholder)
            .Finalize(&g, &summary));

        Node *extrasummary;
        TF_ASSERT_OK(
            NodeBuilder("extra_summary_op", "Ha3ExtraSummaryOp")
            .Input(placeholder)
            .Input(summary)
            .Finalize(&g, &extrasummary));
        g.ToGraphDef(&_graph);
    }

protected:
    void initSearcherSessionResource() {
        auto searcherSessionResource = dynamic_pointer_cast<SearcherSessionResource>(
                _sessionResource);
        ASSERT_TRUE(searcherSessionResource != nullptr);
        searcherSessionResource->searcherResource = _searcherResource;
    }

    void initSearcherQueryResource() {
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(
                getQueryResource());
        ASSERT_TRUE(searcherQueryResource != nullptr);
        searcherQueryResource->sessionMetricsCollector = _collector.get();
        searcherQueryResource->setPool(&_pool);
        searcherQueryResource->setIndexSnapshot(_snapshotPtr);
        searcherQueryResource->indexPartitionReaderWrapper.reset(
                new IndexPartitionReaderWrapper(NULL, NULL, {_snapshotPtr->GetIndexPartitionReader("summary")}));
    }

    Ha3RequestVariant fakeHa3Request(const vector<int32_t>& pks) {
        common::DocIdClause *docIdClause = _request.get()->getDocIdClause();
        docIdClause->setSummaryProfileName("daogou");
        for(auto pk : pks) {
            docIdClause->addGlobalId(GlobalIdentifier(-1, 0, versionid_t(0), 0, pk, 0));
        }
        _request->setDocIdClause(docIdClause);

        Ha3RequestVariant variant(_request, &_pool);
        return variant;
    }

    Ha3RequestVariant fakeHa3RequestByDocid(const vector<int32_t>& docids) {
        common::DocIdClause *docIdClause = _request.get()->getDocIdClause();

        _request->getConfigClause()->setFetchSummaryType(BY_DOCID);
        docIdClause->setSummaryProfileName("daogou");
        for(auto docid : docids) {
            docIdClause->addGlobalId(GlobalIdentifier(docid, 0, versionid_t(0), 0, 0, 0));
        }
        _request->setDocIdClause(docIdClause);

        Ha3RequestVariant variant(_request, &_pool);
        return variant;
    }

    void checkSummaryHit(Hits *hits, const std::string &expectedStr) {
        ASSERT_TRUE(hits != nullptr);
        const auto &expectedDocs = autil::StringUtil::split(expectedStr, ";");
        EXPECT_EQ(expectedDocs.size(), hits->size());
        for (size_t i = 0; i < expectedDocs.size(); i++) {
            auto expectedDocStr = expectedDocs[i];
            if (expectedDocStr == "-") {
                auto hit = hits->getHit(i);
                ASSERT_TRUE(hit != nullptr);
                auto* summaryHit = hit->getSummaryHit();
                ASSERT_TRUE(!summaryHit) << ":the " << i << "th hit";
                continue;
            }
            const auto &fieldKvStrs = autil::StringUtil::split(expectedDocStr, ",");
            for(auto fieldKv : fieldKvStrs) {
                const auto &kv = autil::StringUtil::split(fieldKv, ":");
                EXPECT_EQ(2u, kv.size());
                auto hit = hits->getHit(i);
                ASSERT_TRUE(hit != nullptr);
                auto* summaryHit = hit->getSummaryHit();
                ASSERT_TRUE(summaryHit) << ":the " << i << "th hit";
                summaryHit->setHitSummarySchema(_hitSummarySchema.get());
                EXPECT_EQ(kv[1], hit->getSummaryValue(kv[0])) << ":the " << i << "th hit";
            }
        }
    }

    void checkSummary(const vector<int32_t>& pks, const string& expect) {
        TearDown();
        SetUp();
        prepareSummaryOp();
        initSearcherSessionResource();
        initSearcherQueryResource();
        Variant variant = fakeHa3Request(pks);
        SessionOptions opts;
        opts.sessionResource = _sessionResource;
        Session* sess = NewSession(opts);
        vector<pair<string, Tensor>> inputs;
        vector<Tensor> outputs;
        auto requestTensor = Tensor(DT_VARIANT, TensorShape({}));
        requestTensor.scalar<Variant>()() = variant;
        inputs.push_back(make_pair("ha3_request", requestTensor));
        tensorflow::RunOptions runOptions;
        runOptions.mutable_run_id()->set_value(0);
        runOptions.set_inter_op_thread_pool(-1);
        TF_CHECK_OK(sess->Create(_graph));
        TF_CHECK_OK(sess->Run(runOptions, inputs, {"extra_summary_op"}, {"extra_summary_op"}, &outputs, nullptr));
        ASSERT_EQ(1u, outputs.size());
        auto resultTensor = outputs[0];
        auto ha3ResultVariant = outputs[0].scalar<Variant>()().get<Ha3ResultVariant>();
        ASSERT_TRUE(ha3ResultVariant != nullptr);

        auto result = ha3ResultVariant->getResult();
        ASSERT_TRUE(result != nullptr);
        auto hits = result->getHits();
        ASSERT_TRUE(hits != nullptr);
        checkSummaryHit(hits, expect);
    }

private:
    Ha3SummaryOp *_summaryOp = nullptr;
    Ha3SummaryOp *_extraSummaryOp = nullptr;
    RequestPtr _request;
    TableInfoPtr _tableInfoPtr;
    IndexPartitionWrapperPtr _indexPartPtr;
    PartitionReaderSnapshotPtr _snapshotPtr;
    HitSummarySchemaPtr _hitSummarySchema;
    HitSummarySchemaPoolPtr _hitSummarySchemaPool;
    SessionMetricsCollectorPtr _collector;
    FunctionInterfaceCreatorPtr _funcCreator;
    SearcherResourcePtr _searcherResource;
    ResourceReaderPtr _resourceReaderPtr;
    SummaryProfileManagerPtr _summaryProfileManagerPtr;
    FakeSummaryExtractor *_fakeSummaryExtractor;
    string _rootPath = __FILE__;
    GraphDef _graph;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3ExtraSummaryOpTest);

TEST_F(Ha3ExtraSummaryOpTest, testExtraSummaryOp) {
    prepareSummaryOp();
    initSearcherSessionResource();
    initSearcherQueryResource();
    Variant variant = fakeHa3Request({0, 1, 2, 3});
    SessionOptions opts;
    opts.sessionResource = _sessionResource;
    Session* sess = NewSession(opts);
    vector<pair<string, Tensor>> inputs;
    vector<Tensor> outputs;
    auto requestTensor = Tensor(DT_VARIANT, TensorShape({}));
    requestTensor.scalar<Variant>()() = variant;
    inputs.push_back(make_pair("ha3_request", requestTensor));
    tensorflow::RunOptions runOptions;
    runOptions.mutable_run_id()->set_value(0);
    runOptions.set_inter_op_thread_pool(-1);
    TF_CHECK_OK(sess->Create(_graph));
    TF_CHECK_OK(sess->Run(runOptions, inputs, {"extra_summary_op"}, {"extra_summary_op"}, &outputs, nullptr));
    ASSERT_EQ(1u, outputs.size());
    auto resultTensor = outputs[0];
    auto ha3ResultVariant = outputs[0].scalar<Variant>()().get<Ha3ResultVariant>();
    ASSERT_TRUE(ha3ResultVariant != nullptr);

    auto result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    auto hits = result->getHits();
    ASSERT_TRUE(hits != nullptr);
    checkSummaryHit(hits, "pk:0,attr:0,summary:s0;pk:1,attr:1,summary:s1;pk:2,attr:2,summary:s2;pk:3,attr:3,summary:s3");
    auto queryResource = _sessionResource->getQueryResource(0);
    auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(queryResource);
    ASSERT_TRUE(searcherQueryResource != nullptr);
    auto collectorPtr = searcherQueryResource->sessionMetricsCollector;
    ASSERT_TRUE(collectorPtr != nullptr);
    ASSERT_EQ(2, collectorPtr->getExtraReturnCount());
}

TEST_F(Ha3ExtraSummaryOpTest, testExtraSummaryOpMore) {
    checkSummary({0, 2}, "pk:0,attr:0,summary:s0;pk:2,attr:2,summary:s2;");
    checkSummary({1, 3}, "pk:1,attr:1,summary:s1;pk:3,attr:3,summary:s3;");
    checkSummary({0, 1}, "pk:0,attr:0,summary:s0;pk:1,attr:1,summary:s1;");
    checkSummary({1, 0}, "pk:1,attr:1,summary:s1;pk:0,attr:0,summary:s0;");

}

TEST_F(Ha3ExtraSummaryOpTest, testExtraSummaryOpDegradation) {
    prepareSummaryOp();
    initSearcherSessionResource();
    initSearcherQueryResource();
    Variant variant = fakeHa3Request({0,1,2,3,4});
    SessionOptions opts;
    auto queryResource = _sessionResource->getQueryResource(0);
    queryResource->setDegradeLevel(1.0);
    opts.sessionResource = _sessionResource;
    Session* sess = NewSession(opts);
    vector<pair<string, Tensor>> inputs;
    vector<Tensor> outputs;
    auto requestTensor = Tensor(DT_VARIANT, TensorShape({}));
    requestTensor.scalar<Variant>()() = variant;
    inputs.push_back(make_pair("ha3_request", requestTensor));
    tensorflow::RunOptions runOptions;
    runOptions.mutable_run_id()->set_value(0);
    runOptions.set_inter_op_thread_pool(-1);
    TF_CHECK_OK(sess->Create(_graph));
    TF_CHECK_OK(sess->Run(runOptions, inputs, {"extra_summary_op"}, {"extra_summary_op"}, &outputs, nullptr));
    ASSERT_EQ(1u, outputs.size());
    auto resultTensor = outputs[0];
    auto ha3ResultVariant = outputs[0].scalar<Variant>()().get<Ha3ResultVariant>();
    ASSERT_TRUE(ha3ResultVariant != nullptr);

    auto result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    auto hits = result->getHits();
    ASSERT_TRUE(hits != nullptr);
    checkSummaryHit(hits, "pk:0,attr:0,summary:s0;-;pk:2,attr:2,summary:s2;-;-");
    auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(queryResource);
    ASSERT_TRUE(searcherQueryResource != nullptr);
    auto collectorPtr = searcherQueryResource->sessionMetricsCollector;
    ASSERT_TRUE(collectorPtr != nullptr);
    ASSERT_EQ(true, collectorPtr->isExtraPhase2DegradationQps());
}

TEST_F(Ha3ExtraSummaryOpTest, testExtraSummaryOpByDocId) {
    prepareSummaryOp();
    initSearcherSessionResource();
    initSearcherQueryResource();
    Variant variant = fakeHa3RequestByDocid({0,1,2,3,4,5});
    SessionOptions opts;
    opts.sessionResource = _sessionResource;
    Session* sess = NewSession(opts);
    vector<pair<string, Tensor>> inputs;
    vector<Tensor> outputs;
    auto requestTensor = Tensor(DT_VARIANT, TensorShape({}));
    requestTensor.scalar<Variant>()() = variant;
    inputs.push_back(make_pair("ha3_request", requestTensor));
    tensorflow::RunOptions runOptions;
    runOptions.mutable_run_id()->set_value(0);
    runOptions.set_inter_op_thread_pool(-1);
    TF_CHECK_OK(sess->Create(_graph));
    TF_CHECK_OK(sess->Run(runOptions, inputs, {"extra_summary_op"}, {"extra_summary_op"}, &outputs, nullptr));
    ASSERT_EQ(1u, outputs.size());
    auto resultTensor = outputs[0];
    auto ha3ResultVariant = outputs[0].scalar<Variant>()().get<Ha3ResultVariant>();
    ASSERT_TRUE(ha3ResultVariant != nullptr);

    auto result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    auto hits = result->getHits();
    ASSERT_TRUE(hits != nullptr);
    checkSummaryHit(hits, "pk:0,attr:0,summary:s0;pk:2,attr:2,summary:s2;-;-;-;-");

}

TEST_F(Ha3ExtraSummaryOpTest, testExtraSummaryOpFail) {
    // BY_DOCID
    // hit schema consistant

}

END_HA3_NAMESPACE();
