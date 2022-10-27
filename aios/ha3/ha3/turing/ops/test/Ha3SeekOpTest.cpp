#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <suez/turing/common/CavaConfig.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>
#include <ha3/turing/ops/Ha3SeekOp.h>
#include <ha3/turing/ops/test/MockCavaJitWrapper.h>
#include <ha3/service/SearcherResource.h>
#include <ha3/rank/RankProfileManager.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <ha3/qrs/RequestValidateProcessor.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <suez/turing/expression/function/FunctionInterfaceCreator.h>
#include <ha3_sdk/testlib/index/FakeIndexPartition.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;
using namespace build_service::plugin;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(sorter);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(func_expression);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(qrs);

BEGIN_HA3_NAMESPACE(turing);


class Ha3SeekOpTest : public Ha3OpTestBase {

public:
    void SetUp() override {
        OpTestBase::SetUp();
        // init _tableInfo
        ::cava::CavaJit::globalInit();
        _tableInfo = TableInfoConfigurator().createFromFile(
                TEST_DATA_PATH "/testSeekOp/schema.json");
        _snapshotPtr.reset(new PartitionReaderSnapshot(&_emptyTableMem2IdMap,
                        &_emptyTableMem2IdMap, &_emptyTableMem2IdMap,
                        std::vector<IndexPartitionReaderInfo>()));
        _searcherSessionResource = nullptr;
    }

    void TearDown() override {
        releaseResources();
        OpTestBase::TearDown();
    }

    void initResources() {
        _seekOp = NULL;

        // init _collector
        _collector.reset(new SessionMetricsCollector(NULL));

        // init _funcCreator
        _funcCreator.reset(new FunctionInterfaceCreator);

        // init _commonResource
        _commonResource.reset(new SearchCommonResource(&_pool, _tableInfo,
                        _collector.get(), NULL, NULL,
                        _funcCreator.get(), CavaPluginManagerPtr(),
                        _request.get(), NULL, _cavaJitModules));

        // init _partitionReaderWrapper
        FakeIndex fakeIndex;
        fakeIndex.versionId = 1;
        fakeIndex.indexes["pk"] = "1:1,2:2,3:3,4:4,5:5,6:6;\n";
        fakeIndex.indexes["company_id"] = "1:0;1;2;\n"
                                          "2:3;4;5;\n";
        fakeIndex.attributes = "id : int32_t : 1, 2, 3, 4, 5, 6;"
                               "company_id : int32_t : 1, 1, 1, 2, 2, 2;"
                               "cat_id : int32_t : 1, 2, 6, 3, 4, 5;";
        _partitionReaderWrapper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);

        // init _partitionResource
        _partitionResource.reset(
                new SearchPartitionResource(*_commonResource, _request.get(),
                        _partitionReaderWrapper, _snapshotPtr));

        _attrExprCreator.reset(new FakeAttributeExpressionCreator(&_pool,
                        _partitionReaderWrapper, NULL, NULL, NULL, NULL, NULL));
        _partitionResource->attributeExpressionCreator = _attrExprCreator;

        // init _resourceReader
        _resourceReader.reset(new ResourceReader(TEST_DATA_PATH"/searcher_test"));

        // init _rankProfileManager
        initRankProfileManager(_rankProfileManager, 100u, 10u);

        // init _runtimeResource
        _runtimeResource = createSearchRuntimeResource(_request.get(), _rankProfileManager.get(),
                _commonResource, _partitionResource->attributeExpressionCreator.get());

        // init _sorterManager
        SorterConfig finalSorterConfig;
        // add buildin sorters
        build_service::plugin::ModuleInfo moduleInfo;
        finalSorterConfig.modules.push_back(moduleInfo);
        suez::turing::SorterInfo sorterInfo;
        sorterInfo.sorterName = "DEFAULT";
        sorterInfo.className = "DefaultSorter";
        finalSorterConfig.sorterInfos.push_back(sorterInfo);
        sorterInfo.sorterName = "NULL";
        sorterInfo.className = "NullSorter";
        finalSorterConfig.sorterInfos.push_back(sorterInfo);
        _sorterManager = SorterManager::create(_resourceReader->getConfigPath(),
                finalSorterConfig);


        // init _searcherResource
        _searcherResource.reset(new HA3_NS(service)::SearcherResource);
        _searcherResource->setTableInfo(_tableInfo);
        //_searcherResource->setIndexPartitionWrapper(_partitionReaderWrapper);
        _searcherResource->setRankProfileManager(_rankProfileManager);
        _searcherResource->setSorterManager(_sorterManager);
        _searcherResource->setClusterName("cluster1");
        proto::Range range;
        range.set_from(1);
        range.set_to(1000);
        _searcherResource->setHashIdRange(range);
    }

    void releaseResources()
    {
        if (_searcherSessionResource) {
            _searcherSessionResource->searcherResource.reset();
        }
        _attrExprCreator.reset();
        _snapshotPtr.reset();
        _searcherCacheInfo.reset();
        _searcherResource.reset();
        _sorterManager.reset();
        _runtimeResource.reset();
        _rankProfileManager.reset();
        _resourceReader.reset();
        _partitionResource.reset();
        _partitionReaderWrapper.reset();
        _commonResource.reset();
        _cavaJitModules.clear();
        _funcCreator.reset();
        _collector.reset();
        _seekOp = NULL;
        _tableInfo.reset();
        _request.reset();
    }

    RequestPtr prepareRequest(const string &query,
                              const TableInfoPtr &tableInfoPtr,
                              const string &defaultIndexName)
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(query,
                defaultIndexName);
        if (!requestPtr) {
            return requestPtr;
        }

        requestPtr->setPool(&_pool);
        RequestValidateProcessor::fillPhaseOneInfoMask(requestPtr->getConfigClause(),
                tableInfoPtr->getPrimaryKeyIndexInfo());

        ClusterTableInfoMapPtr clusterTableInfoMapPtr(new ClusterTableInfoMap);
        (*clusterTableInfoMapPtr)["cluster1.default"] = tableInfoPtr;
        ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap);
        ClusterConfigInfo clusterInfo1;
        clusterConfigMapPtr->insert(make_pair("cluster1.default", clusterInfo1));

        ClusterFuncMapPtr clusterFuncMap(new ClusterFuncMap);
        FunctionInterfaceCreator creator;
        FuncConfig fc;
        creator.init(fc, {});
        (*clusterFuncMap)["cluster1.default"] = creator.getFuncInfoMap();
        RequestValidator requestValidator(clusterTableInfoMapPtr,
                10000, clusterConfigMapPtr,
                clusterFuncMap, CavaPluginManagerMapPtr(),
                ClusterSorterNamesPtr(new ClusterSorterNames));
        bool ret = requestValidator.validate(requestPtr);
        if (!ret) {
            return RequestPtr();
        }

        return requestPtr;
    }

    void initRankProfileManager(RankProfileManagerPtr &rankProfileMgrPtr,
                                uint32_t rankSize, uint32_t rerankSize) {
        PlugInManagerPtr nullPtr;
        rankProfileMgrPtr.reset(new RankProfileManager(nullPtr));
        ScorerInfo scorerInfo;
        scorerInfo.scorerName = "DefaultScorer";
        scorerInfo.rankSize = rankSize;
        RankProfile *rankProfile = new RankProfile(DEFAULT_RANK_PROFILE);
        rankProfile->addScorerInfo(scorerInfo);
        scorerInfo.rankSize = rerankSize;
        rankProfile->addScorerInfo(scorerInfo); //two scorer
        rankProfileMgrPtr->addRankProfile(rankProfile);

        CavaPluginManagerPtr cavaPluginManagerPtr;
        rankProfileMgrPtr->init(_resourceReader, cavaPluginManagerPtr, NULL);
    }


    SearchRuntimeResourcePtr createSearchRuntimeResource(
            const common::Request *request,
            RankProfileManager *rankProfileMgr,
            const SearchCommonResourcePtr &commonResource,
            AttributeExpressionCreator *attributeExpressionCreator)
    {
        SearchRuntimeResourcePtr runtimeResource;
        const RankProfile *rankProfile = NULL;
        if (!rankProfileMgr->getRankProfile(request, rankProfile, commonResource->errorResult)) {
            HA3_LOG(WARN, "Get RankProfile Failed");
            //return runtimeResource;
        }
        SortExpressionCreatorPtr exprCreator(new SortExpressionCreator(
                        attributeExpressionCreator, rankProfile,
                        commonResource->matchDocAllocator.get(),
                        commonResource->errorResult, commonResource->pool));
        if (!exprCreator->init(request)) {
            HA3_LOG(WARN, "init sort expression creator failed.");
            //return runtimeResource;
        }
        runtimeResource.reset(new SearchRuntimeResource());
        runtimeResource->sortExpressionCreator = exprCreator;
        runtimeResource->comparatorCreator.reset(new ComparatorCreator(
                        commonResource->pool,
                        request->getConfigClause()->isOptimizeComparator()));
        runtimeResource->docCountLimits.init(request, rankProfile,
                _clusterConfigInfo,
                _partCount, commonResource->tracer);
        return runtimeResource;
    }

    void prepareSeekOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3SeekOp")
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
        _seekOp = dynamic_cast<Ha3SeekOp *>(kernel_.get());
    }

protected:
    void initSearcherSessionResource() {
        _searcherSessionResource = dynamic_cast<SearcherSessionResource*>(
                _sessionResource.get());
        ASSERT_TRUE(_searcherSessionResource != nullptr);
        _searcherSessionResource->searcherResource = _searcherResource;
    }

    void initSearcherQueryResource() {
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(
                getQueryResource());
        ASSERT_TRUE(searcherQueryResource != nullptr);

        searcherQueryResource->indexPartitionReaderWrapper = _partitionReaderWrapper;
        searcherQueryResource->commonResource = _commonResource;
        searcherQueryResource->partitionResource = _partitionResource;
        searcherQueryResource->runtimeResource = _runtimeResource;
        searcherQueryResource->searcherCacheInfo = _searcherCacheInfo;
        searcherQueryResource->sessionMetricsCollector = _collector.get();;
        searcherQueryResource->setPool(&_pool);

    }

    Ha3RequestVariant fakeHa3Request(const std::string& query, const std::string& defaultIndex) {
        _request = prepareRequest(query, _tableInfo, defaultIndex);
        Ha3RequestVariant variant(_request, &_pool);
        return variant;
    }

private:
    Ha3SeekOp *_seekOp = nullptr;
    RequestPtr _request;
    TableInfoPtr _tableInfo;
    SessionMetricsCollectorPtr _collector;
    FunctionInterfaceCreatorPtr _funcCreator;
    std::map<size_t, ::cava::CavaJitModulePtr> _cavaJitModules;
    SearcherSessionResource *_searcherSessionResource = nullptr;
    SearchCommonResourcePtr _commonResource;

    IndexPartitionReaderWrapperPtr _partitionReaderWrapper;
    SearchPartitionResourcePtr _partitionResource;

    config::ResourceReaderPtr _resourceReader;
    RankProfileManagerPtr _rankProfileManager;
    config::ClusterConfigInfo _clusterConfigInfo;
    uint32_t _partCount = 1;
    std::tr1::shared_ptr<SearchRuntimeResource> _runtimeResource;

    SorterManagerPtr _sorterManager;
    SearcherResourcePtr _searcherResource;
    SearcherCacheInfoPtr _searcherCacheInfo;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    IE_NAMESPACE(partition)::TableMem2IdMap _emptyTableMem2IdMap;
    AttributeExpressionCreatorPtr _attrExprCreator;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3SeekOpTest);

TEST_F(Ha3SeekOpTest, testProcessRequest) {
    string query = "config=cluster:cluster1&&query=company_id:1";
    _request = prepareRequest(query, _tableInfo, "company_id");
    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();
    const rank::RankProfile *rankProfile = nullptr;
    ASSERT_TRUE(_rankProfileManager->getRankProfile(_request.get(), rankProfile,
                    _commonResource->errorResult));
    InnerSearchResult innerResult(&_pool);
    auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
     bool ret = Ha3SeekOp::processRequest(_request.get(), rankProfile, searcherQueryResource.get(),
            _searcherResource.get(), innerResult);

    ASSERT_TRUE(ret);
    auto &matchDocVec = innerResult.matchDocVec;
    ASSERT_EQ(3u, matchDocVec.size());
    ASSERT_EQ(0, matchDocVec[0].getDocId());
    ASSERT_EQ(1, matchDocVec[1].getDocId());
    ASSERT_EQ(2, matchDocVec[2].getDocId());
}

TEST_F(Ha3SeekOpTest, testConstructErrorResult) {
    string query = "config=cluster:cluster1&&query=company_id:111";
    _request = prepareRequest(query, _tableInfo, "company_id");
    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();
    const rank::RankProfile *rankProfile = nullptr;
    ASSERT_TRUE(_rankProfileManager->getRankProfile(_request.get(), rankProfile,
                    _commonResource->errorResult));
    InnerSearchResult innerResult(&_pool);
    auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
    auto result = Ha3SeekOp::constructErrorResult(_request.get(), _searcherSessionResource,
            searcherQueryResource.get(),
            _searcherResource.get(), _commonResource.get());
    ASSERT_TRUE(result != nullptr);
    ASSERT_TRUE(result->getTracer() != nullptr);
    ASSERT_TRUE(result->getMatchDocs() != nullptr);
    auto ranges = result->getCoveredRanges();
    ASSERT_EQ(1, ranges.size());
    cout << ranges.begin()->first<<endl;
    ASSERT_TRUE(ranges.find("cluster1") != ranges.end());
    ASSERT_EQ(1, ranges["cluster1"].size());
    ASSERT_EQ(1, ranges["cluster1"][0].first);
    ASSERT_EQ(1000, ranges["cluster1"][0].second);

}

TEST_F(Ha3SeekOpTest, testOpSimple) {
    prepareSeekOp();

    string query = "config=cluster:cluster1,start:0,hit:100&&query=company_id:1";
    string defaultIndex = "company_id";
    Variant variant = fakeHa3Request(query, defaultIndex);
    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant = pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    vector<MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(3u, matchDocVec.size());
    ASSERT_EQ(0, matchDocVec[0].getDocId());
    ASSERT_EQ(1, matchDocVec[1].getDocId());
    ASSERT_EQ(2, matchDocVec[2].getDocId());

    auto pExtraMatchDocsOutputTensor = GetOutput(1);
    ASSERT_TRUE(pExtraMatchDocsOutputTensor != nullptr);
    auto basicValue1 = pExtraMatchDocsOutputTensor ->scalar<uint32_t>()();
    EXPECT_EQ(100u, basicValue1);

    auto pTotalMatchDocsOutputTensor = GetOutput(2);
    ASSERT_TRUE(pTotalMatchDocsOutputTensor != nullptr);
    auto basicValue2 = pTotalMatchDocsOutputTensor->scalar<uint32_t>()();
    EXPECT_EQ(3u, basicValue2);

    auto pActualMatchDocsOutputTensor = GetOutput(3);
    ASSERT_TRUE(pActualMatchDocsOutputTensor != nullptr);
    auto basicValue3 = pActualMatchDocsOutputTensor->scalar<uint32_t>()();
    EXPECT_EQ(3u, basicValue3);

    auto pHa3AggResultsOutputTensor = GetOutput(4);
    ASSERT_TRUE(pHa3AggResultsOutputTensor != nullptr);
}


TEST_F(Ha3SeekOpTest, testOpCompile) {
    prepareSeekOp();

    string query = "config=cluster:cluster1,start:0,hit:100&&query=company_id:1";
    string defaultIndex = "company_id";
    Variant variant = fakeHa3Request(query, defaultIndex);

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();
    auto searcherSessionResource = dynamic_pointer_cast<SearcherSessionResource>(
            _sessionResource);
    CavaConfig cavaConfig;
    MockCavaJitWrapper *cavaJitWrapper = new MockCavaJitWrapper("", cavaConfig);
    //ASSERT_TRUE(cavaJitWrapper->init());
    searcherSessionResource->cavaJitWrapper.reset(cavaJitWrapper);

    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant = pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    vector<MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(3u, matchDocVec.size());
    ASSERT_EQ(0, matchDocVec[0].getDocId());
    ASSERT_EQ(1, matchDocVec[1].getDocId());
    ASSERT_EQ(2, matchDocVec[2].getDocId());

    auto pExtraMatchDocsOutputTensor = GetOutput(1);
    ASSERT_TRUE(pExtraMatchDocsOutputTensor != nullptr);
    auto basicValue1 = pExtraMatchDocsOutputTensor ->scalar<uint32_t>()();
    EXPECT_EQ(100u, basicValue1);

    auto pTotalMatchDocsOutputTensor = GetOutput(2);
    ASSERT_TRUE(pTotalMatchDocsOutputTensor != nullptr);
    auto basicValue2 = pTotalMatchDocsOutputTensor->scalar<uint32_t>()();
    EXPECT_EQ(3u, basicValue2);

    auto pActualMatchDocsOutputTensor = GetOutput(3);
    ASSERT_TRUE(pActualMatchDocsOutputTensor != nullptr);
    auto basicValue3 = pActualMatchDocsOutputTensor->scalar<uint32_t>()();
    EXPECT_EQ(3u, basicValue3);

    auto pHa3AggResultsOutputTensor = GetOutput(4);
    ASSERT_TRUE(pHa3AggResultsOutputTensor != nullptr);
}

END_HA3_NAMESPACE();
