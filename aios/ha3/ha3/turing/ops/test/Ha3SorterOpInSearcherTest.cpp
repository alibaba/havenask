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
#include <ha3/turing/ops/Ha3SorterOp.h>
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


class Ha3SorterOpInSearcherTest : public Ha3OpTestBase {
private:
    Ha3SorterOp *_sorterOp = nullptr;

    RequestPtr _request;

    TableInfoPtr _tableInfo;

    SessionMetricsCollectorPtr _collector;
    IndexPartitionReaderWrapperPtr _partitionReaderWrapper;
    FunctionInterfaceCreatorPtr _funcCreator;
    std::map<size_t, ::cava::CavaJitModulePtr> _cavaJitModules;
    config::ClusterConfigInfo _clusterConfigInfo;
    uint32_t _partCount = 1;
    SearchCommonResourcePtr _commonResource;
    SearchPartitionResourcePtr _partitionResource;
    ResourceReaderPtr _resourceReader;
    RankProfileManagerPtr _rankProfileManager;
    SearchRuntimeResourcePtr _runtimeResource;
    SorterManagerPtr _sorterManager;
    SearcherResourcePtr _searcherResource;
    SearcherCacheInfoPtr _searcherCacheInfo;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    IE_NAMESPACE(partition)::TableMem2IdMap _emptyTableMem2IdMap;
    AttributeExpressionCreatorPtr _attrExprCreator;
public:
    void SetUp() override {
        OpTestBase::SetUp();
        _snapshotPtr.reset(new PartitionReaderSnapshot(&_emptyTableMem2IdMap,
                        &_emptyTableMem2IdMap, &_emptyTableMem2IdMap,
                        std::vector<IndexPartitionReaderInfo>()));
        _tableInfo = TableInfoConfigurator().createFromFile(
                TEST_DATA_PATH "/testSorterOp/schema.json");
    }

    void TearDown() override {
        _tableInfo.reset();
        _attrExprCreator.reset();
        _snapshotPtr.reset();
        OpTestBase::TearDown();
    }

    void initResources() {

        // init _collector
        _collector.reset(new SessionMetricsCollector(NULL));

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

        // init _funcCreator
        _funcCreator.reset(new FunctionInterfaceCreator);

        // init _commonResource
        _commonResource.reset(new SearchCommonResource(&_pool, _tableInfo,
                        _collector.get(), NULL, NULL,
                        _funcCreator.get(), CavaPluginManagerPtr(),
                        _request.get(), NULL, _cavaJitModules));

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
        _searcherResource->setRankProfileManager(_rankProfileManager);
        _searcherResource->setSorterManager(_sorterManager);
    }

    void releaseResources()
    {
        _searcherResource.reset();
        _sorterManager.reset();
        _runtimeResource.reset();
        _rankProfileManager.reset();
        _resourceReader.reset();
        _commonResource.reset();
        _partitionResource.reset();
        _partitionReaderWrapper.reset();
        _funcCreator.reset();
        _collector.reset();
        _sorterOp = NULL;
    }

    RequestPtr prepareRequest(const string &query,
                              const TableInfoPtr &tableInfoPtr,
                              const string &defaultIndexName,
                              bool needValidate = true)
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(query,
                defaultIndexName);
        if (!requestPtr) {
            return requestPtr;
        }

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

        if (needValidate) {
            RequestValidator requestValidator(clusterTableInfoMapPtr,
                    10000, clusterConfigMapPtr,
                    clusterFuncMap, CavaPluginManagerMapPtr(),
                    ClusterSorterNamesPtr(new ClusterSorterNames));
            bool ret = requestValidator.validate(requestPtr);
            if (!ret) {
                return RequestPtr();
            }
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

    void prepareSorterOp(const std::string &attrLocation) {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3SorterOp")
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_UINT32))
                .Input(FakeInput(DT_UINT32))
                .Input(FakeInput(DT_UINT32))
                .Input(FakeInput(DT_UINT16))
                .Attr("attr_location", attrLocation)
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
        _sorterOp = dynamic_cast<Ha3SorterOp *>(kernel_.get());
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

        searcherQueryResource->indexPartitionReaderWrapper = _partitionReaderWrapper;
        searcherQueryResource->commonResource = _commonResource;
        searcherQueryResource->partitionResource = _partitionResource;
        searcherQueryResource->runtimeResource = _runtimeResource;

        searcherQueryResource->searcherCacheInfo = _searcherCacheInfo;

        searcherQueryResource->sessionMetricsCollector = _collector.get();
        searcherQueryResource->setPool(&_pool);

    }

    Ha3RequestVariant fakeHa3Request(const std::string& query, const std::string& defaultIndex) {
        _request = prepareRequest(query, _tableInfo, defaultIndex);
        Ha3RequestVariant variant(_request, &_pool);
        return variant;
    }

    MatchDocsVariant fakeMatchDocs(std::vector<matchdoc::MatchDoc> &docs) {
        auto matchDocAllocator = _commonResource->matchDocAllocator;
        MatchDocsVariant variant(matchDocAllocator, &_pool);
        variant.stealMatchDocs(docs);
        return variant;
    }

    void checkDocId(const std::vector<matchdoc::MatchDoc> &matchDocVec,
                    const std::string& expectedStr) {
        const vector<string> &expectedDocIdVec = autil::StringUtil::split(expectedStr, ",");
        EXPECT_EQ(matchDocVec.size(), expectedDocIdVec.size());
        for(size_t i = 0; i < matchDocVec.size(); i++) {
            int32_t docId = matchDocVec[i].getDocId();
            int32_t expectedDocId = autil::StringUtil::fromString<int32_t>(expectedDocIdVec[i]);
            EXPECT_EQ(expectedDocId, docId);
        }
    }

    template<typename T>
    void checkAttribute(const std::vector<matchdoc::MatchDoc> &matchDocVec,
                        matchdoc::Reference<T> *ref,
                        const std::string& expectedStr) {
        ASSERT_TRUE(ref != nullptr);
        const vector<string> &expectedAttrVec = autil::StringUtil::split(expectedStr, ",");
        EXPECT_EQ(matchDocVec.size(), expectedAttrVec.size());
        for(size_t i = 0; i < matchDocVec.size(); i++) {
            T value = ref->get(matchDocVec[i]);
            T expectedValue = autil::StringUtil::fromString<T>(expectedAttrVec[i]);
            EXPECT_EQ(expectedValue, value);
        }
    }

    void makeMatchDocs(vector<MatchDoc> &matchDocVec, uint32_t docNum) {
        matchDocVec.clear();

        auto matchDocAllocator = _commonResource->matchDocAllocator;;
        Reference<int32_t> *ref = matchDocAllocator->declare<int32_t>("id");

        for (size_t i = 0; i < docNum; i++) {
            MatchDoc doc = matchDocAllocator->allocate(i);
            matchDocVec.push_back(doc);
            ref->set(doc, i + 1);
        }
    }


private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3SorterOpInSearcherTest);

TEST_F(Ha3SorterOpInSearcherTest, testSortInSearcher) {
    std::string attrLocation = "SL_SEARCHER";
    prepareSorterOp(attrLocation);

    string query = "config=cluster:cluster1&&query=company_id:1&&sort=-id";
    _request = prepareRequest(query, _tableInfo, "company_id");

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    auto matchDocAllocator = _commonResource->matchDocAllocator;
    vector<MatchDoc> matchDocVec;
    uint32_t docNum = 3u;
    for (size_t i = 0; i < docNum; i++) {
        matchDocVec.push_back(matchDocAllocator->allocate(i));
    }
    MatchDocsVariant matchDocsVariant = fakeMatchDocs(matchDocVec);
    EXPECT_EQ(0, matchDocVec.size());

    QueryResourcePtr queryResource = getQueryResource();
    uint32_t extraRankCount = 10;
    uint32_t totalMatchDocs = 100;
    uint32_t actualMatchDocs = 10;

    bool ret = _sorterOp->sortInSearcher(_request, _sessionResource.get(), queryResource.get(),
            &matchDocsVariant, extraRankCount, totalMatchDocs, actualMatchDocs);

    ASSERT_TRUE(ret);

    EXPECT_EQ(100, totalMatchDocs);
    EXPECT_EQ(10, actualMatchDocs);

    matchDocsVariant.stealMatchDocs(matchDocVec);
    EXPECT_EQ(docNum, matchDocVec.size());

    checkDocId(matchDocVec, "2,1,0");

    auto idRef = matchDocAllocator->findReference<int32_t>("id");
    checkAttribute(matchDocVec, idRef, "3,2,1");

    releaseResources();
}

TEST_F(Ha3SorterOpInSearcherTest, testCreateFinalSorterWrapper) {
    {
        std::string attrLocation = "SL_SEARCHER";
        prepareSorterOp(attrLocation);

        string query = "config=cluster:cluster1&&query=company_id:1&&sort=-id";
        _request = prepareRequest(query, _tableInfo, "company_id", false);

        initResources();
        initSearcherSessionResource();
        initSearcherQueryResource();

        auto pSorterWrapper = _sorterOp->createFinalSorterWrapper(
                _request.get(), _sorterManager.get(), &_pool);
        ASSERT_TRUE(pSorterWrapper != nullptr);
        delete pSorterWrapper;
        releaseResources();
    }

    {
        std::string attrLocation = "SL_SEARCHER";
        prepareSorterOp(attrLocation);

        string query = "config=cluster:cluster1&&query=company_id:1&&"
                       "sort=-id&&final_sort=sort_name:NOT_EXIST";
        _request = prepareRequest(query, _tableInfo, "company_id", false);

        initResources();
        initSearcherSessionResource();
        initSearcherQueryResource();

        auto pSorterWrapper = _sorterOp->createFinalSorterWrapper(
                _request.get(), _sorterManager.get(), &_pool);
        ASSERT_TRUE(pSorterWrapper == nullptr);
        delete pSorterWrapper;
        releaseResources();
    }
}

TEST_F(Ha3SorterOpInSearcherTest, testOpSimple) {
    std::string attrLocation = "SL_SEARCHER";
    prepareSorterOp(attrLocation);

    string query = "config=cluster:cluster1,start:0,hit:100&&query=company_id:1&&sort=-id";
    string defaultIndex = "company_id";
    Ha3RequestVariant requestVariant = fakeHa3Request(query, defaultIndex);

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    auto matchDocAllocator = _commonResource->matchDocAllocator;
    vector<MatchDoc> matchDocVec;
    uint32_t docNum = 3u;
    for (size_t i = 0; i < docNum; i++) {
        matchDocVec.push_back(matchDocAllocator->allocate(i));
    }
    MatchDocsVariant matchDocsVariant = fakeMatchDocs(matchDocVec);
    EXPECT_EQ(0, matchDocVec.size());

    uint32_t extraRankCount = 10;
    uint32_t totalMatchDocs = 20;
    uint32_t actualMatchDocs = 20;
    uint16_t srcCount = 1;

    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&matchDocsVariant](int x) -> Variant {
                return matchDocsVariant;
            });

    AddInput<uint32_t>(
            TensorShape({}),
            [&extraRankCount](int x) -> uint32_t {
                return extraRankCount;
            });

    AddInput<uint32_t>(
            TensorShape({}),
            [&totalMatchDocs](int x) -> uint32_t {
                return totalMatchDocs;
            });

    AddInput<uint32_t>(
            TensorShape({}),
            [&actualMatchDocs](int x) -> uint32_t {
                return actualMatchDocs;
            });

    AddInput<uint16_t>(
            TensorShape({}),
            [&srcCount](int x) -> uint16_t {
                return srcCount;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariantOutput = pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariantOutput != nullptr);
    vector<MatchDoc> matchDocVecOutput;
    matchDocsVariantOutput->stealMatchDocs(matchDocVecOutput);
    ASSERT_EQ(3u, matchDocVecOutput.size());
    checkDocId(matchDocVecOutput, "2,1,0");
    auto idRef = matchDocAllocator->findReference<int32_t>("id");
    checkAttribute(matchDocVecOutput, idRef, "3,2,1");

    auto pTotalDocsOutputTensor = GetOutput(1);
    ASSERT_TRUE(pTotalDocsOutputTensor != nullptr);
    auto totalDocs = pTotalDocsOutputTensor ->scalar<uint32_t>()();
    EXPECT_EQ(20u, totalDocs);

    auto pActualDocsOutputTensor = GetOutput(2);
    ASSERT_TRUE(pActualDocsOutputTensor != nullptr);
    auto actualDocs = pActualDocsOutputTensor->scalar<uint32_t>()();
    EXPECT_EQ(20, actualDocs);

    releaseResources();
}

TEST_F(Ha3SorterOpInSearcherTest, testOpSimpleCacheMerger) {
    std::string attrLocation = "SL_SEARCHCACHEMERGER";
    prepareSorterOp(attrLocation);

    string query = "config=cluster:cluster1,start:0,hit:100&&query=company_id:1&&sort=-id";
    string defaultIndex = "company_id";
    Ha3RequestVariant requestVariant = fakeHa3Request(query, defaultIndex);

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    _request->setPool(&_pool);

    auto matchDocAllocator = _commonResource->matchDocAllocator;

    vector<MatchDoc> matchDocVec;
    uint32_t docNum = 3u;
    makeMatchDocs(matchDocVec, docNum);
    MatchDocsVariant matchDocsVariant = fakeMatchDocs(matchDocVec);
    EXPECT_EQ(0, matchDocVec.size());

    uint32_t extraRankCount = 10;
    uint32_t totalMatchDocs = 20;
    uint32_t actualMatchDocs = 20;
    uint16_t srcCount = 1;

    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&matchDocsVariant](int x) -> Variant {
                return matchDocsVariant;
            });

    AddInput<uint32_t>(
            TensorShape({}),
            [&extraRankCount](int x) -> uint32_t {
                return extraRankCount;
            });

    AddInput<uint32_t>(
            TensorShape({}),
            [&totalMatchDocs](int x) -> uint32_t {
                return totalMatchDocs;
            });

    AddInput<uint32_t>(
            TensorShape({}),
            [&actualMatchDocs](int x) -> uint32_t {
                return actualMatchDocs;
            });

    AddInput<uint16_t>(
            TensorShape({}),
            [&srcCount](int x) -> uint16_t {
                return srcCount;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariantOutput = pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariantOutput != nullptr);
    vector<MatchDoc> matchDocVecOutput;
    matchDocsVariantOutput->stealMatchDocs(matchDocVecOutput);
    ASSERT_EQ(3u, matchDocVecOutput.size());
    checkDocId(matchDocVecOutput, "2,1,0");
    auto idRef = matchDocAllocator->findReference<int32_t>("id");
    checkAttribute(matchDocVecOutput, idRef, "3,2,1");

    auto pTotalDocsOutputTensor = GetOutput(1);
    ASSERT_TRUE(pTotalDocsOutputTensor != nullptr);
    auto totalDocs = pTotalDocsOutputTensor ->scalar<uint32_t>()();
    EXPECT_EQ(20u, totalDocs);

    auto pActualDocsOutputTensor = GetOutput(2);
    ASSERT_TRUE(pActualDocsOutputTensor != nullptr);
    auto actualDocs = pActualDocsOutputTensor->scalar<uint32_t>()();
    EXPECT_EQ(20, actualDocs);

    releaseResources();
}

END_HA3_NAMESPACE();
