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
#include <ha3/turing/ops/Ha3SeekAndJoinOp.h>
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
#include <ha3/search/IndexPartitionReaderUtil.h>
#include <indexlib/test/schema_maker.h>
#include <indexlib/test/partition_state_machine.h>
#include <indexlib/config/index_partition_options.h>
#include <indexlib/partition/partition_reader_snapshot.h>
#include <indexlib/partition/index_application.h>

using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;
using namespace build_service::plugin;

IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);

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


class Ha3SeekAndJoinOpTest : public Ha3OpTestBase {
public:
    Ha3SeekAndJoinOpTest() {
        _searcherSessionResource = nullptr;
        _mainRoot = GET_TEMP_DATA_PATH() + "testSeekAndJoinOp/main";
        _auxRoot = GET_TEMP_DATA_PATH() + "testSeekAndJoinOp/aux";
}
    void SetUp() override {
        OpTestBase::SetUp();
	::cava::CavaJit::globalInit();
        _auxMatchDocAllocator.reset(new MatchDocAllocator(&_pool));
        initIndexData();
    }

    void TearDown() override {
        OpTestBase::TearDown();
	_auxMatchDocAllocator.reset();
	releaseResources();
	releaseIndexData();
	_hasAuxPk = true;
    }

    void makeAuxMatchDocs(vector<MatchDoc> &matchDocVec,
            const std::vector<int32_t> &storeIds) {
        matchDocVec.clear();

        auto matchDocAllocator = _auxMatchDocAllocator;
	matchdoc::Reference<int32_t> *ref =
                matchDocAllocator->declare<int32_t>(_joinFieldName);

        for (size_t i = 0; i < storeIds.size(); i++) {
            MatchDoc doc = matchDocAllocator->allocate(i);
            matchDocVec.push_back(doc);
            ref->set(doc, storeIds[i]);
        }
    }

    MatchDocsVariant makeAuxMatchDocsVariant(
            std::vector<matchdoc::MatchDoc> &docs) {
        auto matchDocAllocator = _auxMatchDocAllocator;
        MatchDocsVariant variant(matchDocAllocator, &_pool);
        variant.stealMatchDocs(docs);
        return variant;
    }


    void initResources() {
        _seekAndJoinOp = NULL;

        // init _collector
        _collector.reset(new SessionMetricsCollector(NULL));

        // init _funcCreator
        _funcCreator.reset(new FunctionInterfaceCreator);

        // init _commonResource
        _commonResource.reset(new SearchCommonResource(&_pool, _tableInfo,
                        _collector.get(), NULL, NULL,
                        _funcCreator.get(), CavaPluginManagerPtr(),
                        _request.get(), NULL, _cavaJitModules));

	_matchDocAllocator = _commonResource->matchDocAllocator;

        _partitionReaderWrapper =
                IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
                        _snapshotPtr.get(), _mainTableName);

        // init _partitionResource
        _partitionResource.reset(
                new SearchPartitionResource(*_commonResource, _request.get(),
                        _partitionReaderWrapper, _snapshotPtr));

	_attrExprCreator = _partitionResource->attributeExpressionCreator;

        _resourceReader.reset(new ResourceReader(TEST_DATA_PATH"/searcher_test"));

        initRankProfileManager(_rankProfileManager, 100u, 10u);

        _runtimeResource = createSearchRuntimeResource(_request.get(),
                _rankProfileManager.get(), _commonResource,
                _partitionResource->attributeExpressionCreator.get());


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

        _searcherResource.reset(new SearcherResource);
        _searcherResource->setTableInfo(_tableInfo);
        _searcherResource->setRankProfileManager(_rankProfileManager);
        _searcherResource->setSorterManager(_sorterManager);
	_searcherResource->setFunctionCreator(_funcCreator);

	ClusterConfigInfo clusterConfig;
        clusterConfig._joinConfig._scanJoinCluster = _auxTableName;
        JoinConfigInfo joinConfigInfo(
            true, _auxTableName, _joinFieldName, false, false);
        clusterConfig._joinConfig.addJoinInfo(joinConfigInfo);
        _searcherResource->_clusterConfig = clusterConfig;
    }

    void releaseResources()
    {
	_matchDocAllocator.reset();
        _attrExprCreator.reset();
	_commonResource.reset();
        _snapshotPtr.reset();
        _searcherResource.reset();
        _sorterManager.reset();
        _runtimeResource.reset();
        _rankProfileManager.reset();
        _resourceReader.reset();
        _partitionResource.reset();
        _partitionReaderWrapper.reset();
        _cavaJitModules.clear();
        _funcCreator.reset();
        _collector.reset();
        _seekAndJoinOp = NULL;
        _request.reset();
	_searcherResource.reset();
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
        (*clusterTableInfoMapPtr)["auction.default"] = tableInfoPtr;
        ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap);
        ClusterConfigInfo clusterInfo1;
        clusterConfigMapPtr->insert(make_pair("auction.default", clusterInfo1));

        ClusterFuncMapPtr clusterFuncMap(new ClusterFuncMap);
        FunctionInterfaceCreator creator;
        FuncConfig fc;
        creator.init(fc, {});
        (*clusterFuncMap)["auction.default"] = creator.getFuncInfoMap();
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

    void prepareSeekAndJoinOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3SeekAndJoinOp")
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
        _seekAndJoinOp = dynamic_cast<Ha3SeekAndJoinOp *>(kernel_.get());
    }

protected:
    void initIndexData() {
        fslib::fs::FileSystem::remove(_mainRoot);
        fslib::fs::FileSystem::remove(_auxRoot);
        IndexPartitionOptions options;
        string zoneName = _mainTableName;
        string field = "id:uint64:pk;store_id:int32;cat_id:int32;price:int32";
        string index = "pk:primarykey64:id;store_id:number:store_id;cat_id:number:cat_id;";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
                field, index, "store_id;cat_id;price", "");
        schema->SetSchemaName(zoneName);

	_psm.reset(new PartitionStateMachine);
        ASSERT_TRUE(_psm->Init(schema, options, _mainRoot));

        string fullDocs0 = "cmd=add,id=0,store_id=1,cat_id=1001,price=100;"
	    "cmd=add,id=1,store_id=2,cat_id=1002,price=200;"
	    "cmd=add,id=2,store_id=2,cat_id=1003,price=300;"
	    "cmd=add,id=3,store_id=3,cat_id=1004,price=400;"
	    "cmd=add,id=4,store_id=3,cat_id=1005,price=500;"
	    "cmd=add,id=5,store_id=4,cat_id=1006,price=600;"
	    ;
        ASSERT_TRUE(_psm->Transfer(BUILD_FULL, fullDocs0, "", ""));
        IndexPartitionPtr indexPart = _psm->GetIndexPartition();

        IndexPartitionOptions auxOptions;
        string auxZoneName = _auxTableName;
        string auxField = "aux_id:uint64:pk;store_id:int32;store_price:int32";
        string auxIndex = "store_id:number:store_id;";
	if (_hasAuxPk) {
	    auxIndex += "aux_pk:primarykey64:aux_id;";
	}
        IndexPartitionSchemaPtr auxSchema = SchemaMaker::MakeSchema(
                auxField, auxIndex, "store_id;store_price", "");
        auxSchema->SetSchemaName(auxZoneName);

	_auxPsm.reset(new PartitionStateMachine);
        ASSERT_TRUE(_auxPsm->Init(auxSchema, auxOptions, _auxRoot));

        string auxFullDocs0 = "cmd=add,aux_id=0,store_id=1,store_price=1000;"
	    "cmd=add,aux_id=1,store_id=2,store_price=2000;"
	    "cmd=add,aux_id=2,store_id=2,store_price=3000;"
	    "cmd=add,aux_id=3,store_id=3,store_price=4000;"
	    "cmd=add,aux_id=4,store_id=3,store_price=5000;"
	    "cmd=add,aux_id=5,store_id=3,store_price=6000;"
	    ;
        ASSERT_TRUE(_auxPsm->Transfer(BUILD_FULL, auxFullDocs0, "", ""));
        IndexPartitionPtr auxIndexPart = _auxPsm->GetIndexPartition();

	vector<IndexPartitionPtr> indexPartitions{indexPart, auxIndexPart};
        JoinRelationMap joinRelations;
        joinRelations[_mainTableName] = { JoinField(
                _joinFieldName, _auxTableName, false, false) };

        _indexApp.reset(new IndexApplication);

        ASSERT_TRUE(_indexApp->Init(indexPartitions, joinRelations));
        _snapshotPtr = _indexApp->CreateSnapshot();
        _tableInfo = TableInfoConfigurator::createFromIndexApp(
                _mainTableName, _indexApp);
    }
    void releaseIndexData() {
        _tableInfo.reset();
        if (_searcherSessionResource) {
            _searcherSessionResource->searcherResource.reset();
        }
	_snapshotPtr.reset();
	_indexApp.reset();
	_psm.reset();
	_auxPsm.reset();
        fslib::fs::FileSystem::remove(_mainRoot);
        fslib::fs::FileSystem::remove(_auxRoot);
    }
    void initSearcherSessionResource() {
        auto searcherSessionResource = dynamic_pointer_cast<SearcherSessionResource>(
                _sessionResource);
        ASSERT_TRUE(searcherSessionResource != nullptr);
        searcherSessionResource->searcherResource = _searcherResource;
        _searcherSessionResource = searcherSessionResource.get();
    }

    void initSearcherQueryResource() {
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(
                getQueryResource());
        ASSERT_TRUE(searcherQueryResource != nullptr);
        searcherQueryResource->indexPartitionReaderWrapper = _partitionReaderWrapper;
        searcherQueryResource->commonResource = _commonResource;
        searcherQueryResource->partitionResource = _partitionResource;
        searcherQueryResource->runtimeResource = _runtimeResource;

        searcherQueryResource->sessionMetricsCollector = _collector.get();
        searcherQueryResource->setPool(&_pool);
	searcherQueryResource->setIndexSnapshot(_snapshotPtr);
    }
    Ha3RequestVariant fakeHa3Request(
            const std::string &query, const std::string &defaultIndex) {
        _request = prepareRequest(query, _tableInfo, defaultIndex);
        Ha3RequestVariant variant(_request, &_pool);
        return variant;
    }

private:
    Ha3SeekAndJoinOp *_seekAndJoinOp = nullptr;

    Ha3MatchDocAllocatorPtr _matchDocAllocator;
    MatchDocAllocatorPtr _auxMatchDocAllocator;
    string _mainTableName{"auction"};
    string _auxTableName{"store"};
    string _joinFieldName{"store_id"};

    RequestPtr _request;

    string _mainRoot;
    string _auxRoot;
    SearcherSessionResource *_searcherSessionResource;
    PartitionStateMachinePtr _psm;
    PartitionStateMachinePtr _auxPsm;
    TableInfoPtr _tableInfo;

    SorterManagerPtr _sorterManager;
    SearcherResourcePtr _searcherResource;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    IE_NAMESPACE(partition)::TableMem2IdMap _emptyTableMem2IdMap;
    AttributeExpressionCreatorPtr _attrExprCreator;

    SessionMetricsCollectorPtr _collector;
    FunctionInterfaceCreatorPtr _funcCreator;
    std::map<size_t, ::cava::CavaJitModulePtr> _cavaJitModules;
    SearchCommonResourcePtr _commonResource;

    IndexPartitionReaderWrapperPtr _partitionReaderWrapper;
    SearchPartitionResourcePtr _partitionResource;

    config::ResourceReaderPtr _resourceReader;
    RankProfileManagerPtr _rankProfileManager;
    config::ClusterConfigInfo _clusterConfigInfo;
    uint32_t _partCount = 1;
    std::tr1::shared_ptr<SearchRuntimeResource> _runtimeResource;
    bool _hasAuxPk{true};
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3SeekAndJoinOpTest);

TEST_F(Ha3SeekAndJoinOpTest, testProcessRequest) {
   string query = "config=cluster:auction,start:0,hit:100&&query=cat_id:1001 "
                   "OR cat_id:1002 OR cat_id:1006";
    string defaultIndex = "store_id";
    _request = prepareRequest(query, _tableInfo, "company_id");
    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();
    const rank::RankProfile *rankProfile = nullptr;
    ASSERT_TRUE(_rankProfileManager->getRankProfile(_request.get(), rankProfile,
                    _commonResource->errorResult));
    InnerSearchResult innerResult(&_pool);
    auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());

    bool ret = Ha3SeekAndJoinOp::processRequest(_request.get(), rankProfile,
            searcherQueryResource.get(), _searcherResource.get(), nullptr,  innerResult);
    ASSERT_TRUE(ret);
    auto &matchDocVec = innerResult.matchDocVec;
    ASSERT_EQ(3u, matchDocVec.size());
    ASSERT_EQ(0, matchDocVec[0].getDocId());
    ASSERT_EQ(1, matchDocVec[1].getDocId());
    ASSERT_EQ(5, matchDocVec[2].getDocId());
}

TEST_F(Ha3SeekAndJoinOpTest, testOpSimple) {
    string query = "config=cluster:auction,start:0,hit:100&&query=cat_id:1001 "
                   "OR cat_id:1002 OR cat_id:1006";
    string defaultIndex = "store_id";
    Variant variant = fakeHa3Request(query, defaultIndex);
    vector<MatchDoc> auxDocs;
    makeAuxMatchDocs(auxDocs, {1,2,2,3,3,3});
    MatchDocsVariant auxDocsVariant = makeAuxMatchDocsVariant(auxDocs);

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareSeekAndJoinOp();

    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&auxDocsVariant](int x) -> Variant {
                return auxDocsVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant = pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    vector<MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(4u, matchDocVec.size());
    ASSERT_EQ(0, matchDocVec[0].getDocId());
    ASSERT_EQ(1, matchDocVec[1].getDocId());
    ASSERT_EQ(1, matchDocVec[2].getDocId());
    ASSERT_EQ(5, matchDocVec[3].getDocId());

    auto *expr = _attrExprCreator->createAtomicExpr("store_price");
    auto *storePriceExpr =
            dynamic_cast<AttributeExpressionTyped<int32_t> *>(expr);
    ASSERT_TRUE(storePriceExpr);

    ASSERT_EQ(1000, storePriceExpr->evaluateAndReturn(matchDocVec[0]));
    ASSERT_EQ(2000, storePriceExpr->evaluateAndReturn(matchDocVec[1]));
    ASSERT_EQ(3000, storePriceExpr->evaluateAndReturn(matchDocVec[2]));
    ASSERT_EQ(0, storePriceExpr->evaluateAndReturn(matchDocVec[3]));

    auto pExtraMatchDocsOutputTensor = GetOutput(1);
    ASSERT_TRUE(pExtraMatchDocsOutputTensor != nullptr);
    auto basicValue1 = pExtraMatchDocsOutputTensor ->scalar<uint32_t>()();
    EXPECT_EQ(100u, basicValue1);

    auto pTotalMatchDocsOutputTensor = GetOutput(2);
    ASSERT_TRUE(pTotalMatchDocsOutputTensor != nullptr);
    auto basicValue2 = pTotalMatchDocsOutputTensor->scalar<uint32_t>()();
    EXPECT_EQ(4u, basicValue2);

    auto pActualMatchDocsOutputTensor = GetOutput(3);
    ASSERT_TRUE(pActualMatchDocsOutputTensor != nullptr);
    auto basicValue3 = pActualMatchDocsOutputTensor->scalar<uint32_t>()();
    EXPECT_EQ(4u, basicValue3);

    auto pHa3AggResultsOutputTensor = GetOutput(4);
    ASSERT_TRUE(pHa3AggResultsOutputTensor != nullptr);
}

TEST_F(Ha3SeekAndJoinOpTest, testEmptyAuxResults) {
    string query = "config=cluster:auction,start:0,hit:100&&query=cat_id:1001 "
                   "OR cat_id:1002 OR cat_id:1006";
    string defaultIndex = "store_id";
    Variant variant = fakeHa3Request(query, defaultIndex);
    vector<MatchDoc> auxDocs;
    makeAuxMatchDocs(auxDocs, {});
    MatchDocsVariant auxDocsVariant = makeAuxMatchDocsVariant(auxDocs);

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareSeekAndJoinOp();

    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&auxDocsVariant](int x) -> Variant {
                return auxDocsVariant;
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
    ASSERT_EQ(5, matchDocVec[2].getDocId());

    auto *expr = _attrExprCreator->createAtomicExpr("store_price");
    auto *storePriceExpr =
            dynamic_cast<AttributeExpressionTyped<int32_t> *>(expr);
    ASSERT_TRUE(storePriceExpr);

    ASSERT_EQ(0, storePriceExpr->evaluateAndReturn(matchDocVec[0]));
    ASSERT_EQ(0, storePriceExpr->evaluateAndReturn(matchDocVec[1]));
    ASSERT_EQ(0, storePriceExpr->evaluateAndReturn(matchDocVec[2]));

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

TEST_F(Ha3SeekAndJoinOpTest, testNoAux) {
    string query = "config=cluster:auction,start:0,hit:100&&query=cat_id:1001 "
                   "OR cat_id:1002 OR cat_id:1006";
    string defaultIndex = "store_id";
    Variant variant = fakeHa3Request(query, defaultIndex);
    MatchDocsVariant auxDocsVariant;

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareSeekAndJoinOp();

    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&auxDocsVariant](int x) -> Variant {
                return auxDocsVariant;
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
    ASSERT_EQ(5, matchDocVec[2].getDocId());

    auto *expr = _attrExprCreator->createAtomicExpr("store_price");
    auto *storePriceExpr =
            dynamic_cast<AttributeExpressionTyped<int32_t> *>(expr);
    ASSERT_TRUE(storePriceExpr);
    _matchDocAllocator->extend();

    ASSERT_EQ(2000, storePriceExpr->evaluateAndReturn(matchDocVec[0]));
    ASSERT_EQ(3000, storePriceExpr->evaluateAndReturn(matchDocVec[1]));
    ASSERT_EQ(5000, storePriceExpr->evaluateAndReturn(matchDocVec[2]));

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


TEST_F(Ha3SeekAndJoinOpTest, testOpSimpleStrong) {
    string query = "config=join_type:strong,cluster:auction,start:0,hit:100&&query=cat_id:1001 "
                   "OR cat_id:1002 OR cat_id:1006";
    string defaultIndex = "store_id";
    Variant variant = fakeHa3Request(query, defaultIndex);

    vector<MatchDoc> auxDocs;
    makeAuxMatchDocs(auxDocs, {1,2,2,3,3,3});
    MatchDocsVariant auxDocsVariant = makeAuxMatchDocsVariant(auxDocs);

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareSeekAndJoinOp();

    auto *expr = _attrExprCreator->createAtomicExpr("store_price");
    auto *storePriceExpr =
            dynamic_cast<AttributeExpressionTyped<int32_t> *>(expr);
    ASSERT_TRUE(storePriceExpr);

    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&auxDocsVariant](int x) -> Variant {
                return auxDocsVariant;
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
    ASSERT_EQ(1, matchDocVec[2].getDocId());


    ASSERT_EQ(1000, storePriceExpr->evaluateAndReturn(matchDocVec[0]));
    ASSERT_EQ(2000, storePriceExpr->evaluateAndReturn(matchDocVec[1]));
    ASSERT_EQ(3000, storePriceExpr->evaluateAndReturn(matchDocVec[2]));

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

TEST_F(Ha3SeekAndJoinOpTest, testEmptyAuxResultsStrong) {
    string query = "config=join_type:strong,cluster:auction,start:0,hit:100&&query=cat_id:1001 "
                   "OR cat_id:1002 OR cat_id:1006";
    string defaultIndex = "store_id";
    Variant variant = fakeHa3Request(query, defaultIndex);
    vector<MatchDoc> auxDocs;
    makeAuxMatchDocs(auxDocs, {});
    MatchDocsVariant auxDocsVariant = makeAuxMatchDocsVariant(auxDocs);

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareSeekAndJoinOp();

    auto *expr = _attrExprCreator->createAtomicExpr("store_price");
    auto *storePriceExpr =
            dynamic_cast<AttributeExpressionTyped<int32_t> *>(expr);
    ASSERT_TRUE(storePriceExpr);

    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&auxDocsVariant](int x) -> Variant {
                return auxDocsVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant = pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    vector<MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(0u, matchDocVec.size());

    auto pExtraMatchDocsOutputTensor = GetOutput(1);
    ASSERT_TRUE(pExtraMatchDocsOutputTensor != nullptr);
    auto basicValue1 = pExtraMatchDocsOutputTensor ->scalar<uint32_t>()();
    EXPECT_EQ(100u, basicValue1);

    auto pTotalMatchDocsOutputTensor = GetOutput(2);
    ASSERT_TRUE(pTotalMatchDocsOutputTensor != nullptr);
    auto basicValue2 = pTotalMatchDocsOutputTensor->scalar<uint32_t>()();
    EXPECT_EQ(0, basicValue2);

    auto pActualMatchDocsOutputTensor = GetOutput(3);
    ASSERT_TRUE(pActualMatchDocsOutputTensor != nullptr);
    auto basicValue3 = pActualMatchDocsOutputTensor->scalar<uint32_t>()();
    EXPECT_EQ(0, basicValue3);

    auto pHa3AggResultsOutputTensor = GetOutput(4);
    ASSERT_TRUE(pHa3AggResultsOutputTensor != nullptr);
}

TEST_F(Ha3SeekAndJoinOpTest, testNoAuxStrong) {
    string query = "config=join_type:strong,cluster:auction,start:0,hit:100&&query=cat_id:1001 "
                   "OR cat_id:1002 OR cat_id:1006";
    string defaultIndex = "store_id";
    Variant variant = fakeHa3Request(query, defaultIndex);
    MatchDocsVariant auxDocsVariant;

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareSeekAndJoinOp();

    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&auxDocsVariant](int x) -> Variant {
                return auxDocsVariant;
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
    ASSERT_EQ(5, matchDocVec[2].getDocId());

    auto *expr = _attrExprCreator->createAtomicExpr("store_price");
    auto *storePriceExpr =
            dynamic_cast<AttributeExpressionTyped<int32_t> *>(expr);
    ASSERT_TRUE(storePriceExpr);
    _matchDocAllocator->extend();

    ASSERT_EQ(2000, storePriceExpr->evaluateAndReturn(matchDocVec[0]));
    ASSERT_EQ(3000, storePriceExpr->evaluateAndReturn(matchDocVec[1]));
    ASSERT_EQ(5000, storePriceExpr->evaluateAndReturn(matchDocVec[2]));

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

TEST_F(Ha3SeekAndJoinOpTest, testNoAuxPk) {
    TearDown();
    _hasAuxPk = false;
    SetUp();
    string query = "config=join_type:strong,cluster:auction,start:0,hit:100&&query=cat_id:1001 "
                   "OR cat_id:1002 OR cat_id:1006";
    string defaultIndex = "store_id";
    Variant variant = fakeHa3Request(query, defaultIndex);
    MatchDocsVariant auxDocsVariant;

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareSeekAndJoinOp();

    auto *expr = _attrExprCreator->createAtomicExpr("store_price");
    ASSERT_FALSE(expr);

    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&auxDocsVariant](int x) -> Variant {
                return auxDocsVariant;
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
    ASSERT_EQ(5, matchDocVec[2].getDocId());

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

TEST_F(Ha3SeekAndJoinOpTest, testOpCompile) {
    string query = "config=cluster:auction,start:0,hit:100&&query=cat_id:1001 "
                   "OR cat_id:1002 OR cat_id:1006";
    string defaultIndex = "store_id";
    Variant variant = fakeHa3Request(query, defaultIndex);
    MatchDocsVariant auxDocsVariant;

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareSeekAndJoinOp();

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
    AddInput<Variant>(
            TensorShape({}),
            [&auxDocsVariant](int x) -> Variant {
                return auxDocsVariant;
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
    ASSERT_EQ(5, matchDocVec[2].getDocId());

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

TEST_F(Ha3SeekAndJoinOpTest, testBuildJoinHashInfo) {
    string query = "config=cluster:auction,start:0,hit:100&&query=cat_id:1001 "
                   "OR cat_id:1002 OR cat_id:1006";
    string defaultIndex = "store_id";
    _request = prepareRequest(query, _tableInfo, defaultIndex);

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareSeekAndJoinOp();

    vector<MatchDoc> auxMatchDocs;
    vector<int32_t> storeIds{1,1,2,3,3,4};
    makeAuxMatchDocs(auxMatchDocs, storeIds);
    auto hashJoinInfo = _seekAndJoinOp->buildJoinHashInfo(
            _auxMatchDocAllocator.get(), auxMatchDocs);
    ASSERT_EQ(_auxTableName, hashJoinInfo->getAuxTableName());
    ASSERT_EQ(_joinFieldName, hashJoinInfo->getJoinFieldName());
    ASSERT_EQ(nullptr, hashJoinInfo->getJoinAttrExpr());
    ASSERT_EQ(nullptr, hashJoinInfo->getAuxDocidRef());
    auto &hashJoinMap = hashJoinInfo->getHashJoinMap();
    std::unordered_map<size_t, std::vector<int32_t>> expectedHashMap{
	{1, {0, 1}},
	{2, {2}},
	{3, {3, 4}},
	{4, {5}}
    };
    ASSERT_EQ(expectedHashMap, hashJoinMap);
}

TEST_F(Ha3SeekAndJoinOpTest, testBuildJoinHashInfoNoAuxDocsNoJoinField) {
    string query = "config=cluster:auction,start:0,hit:100&&query=cat_id:1001 "
                   "OR cat_id:1002 OR cat_id:1006";
    string defaultIndex = "store_id";
    _request = prepareRequest(query, _tableInfo, defaultIndex);
    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareSeekAndJoinOp();
    vector<MatchDoc> auxMatchDocs;
    auto hashJoinInfo = _seekAndJoinOp->buildJoinHashInfo(
            _auxMatchDocAllocator.get(), auxMatchDocs);
    ASSERT_NE(nullptr, hashJoinInfo);
    ASSERT_EQ(0, hashJoinInfo->getHashJoinMap().size());
}

TEST_F(Ha3SeekAndJoinOpTest, testBuildJoinHashInfoNoAuxDocs) {
    string query = "config=cluster:auction,start:0,hit:100&&query=cat_id:1001 "
                   "OR cat_id:1002 OR cat_id:1006";
    string defaultIndex = "store_id";
    _request = prepareRequest(query, _tableInfo, defaultIndex);

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareSeekAndJoinOp();
    vector<MatchDoc> auxMatchDocs;
    makeAuxMatchDocs(auxMatchDocs, {});
    auto hashJoinInfo = _seekAndJoinOp->buildJoinHashInfo(
            _auxMatchDocAllocator.get(), auxMatchDocs);
    ASSERT_EQ(_auxTableName, hashJoinInfo->getAuxTableName());
    ASSERT_EQ(_joinFieldName, hashJoinInfo->getJoinFieldName());
    ASSERT_EQ(nullptr, hashJoinInfo->getJoinAttrExpr());
    ASSERT_EQ(nullptr, hashJoinInfo->getAuxDocidRef());
    auto &hashJoinMap = hashJoinInfo->getHashJoinMap();
    ASSERT_TRUE(hashJoinMap.empty());
}

TEST_F(Ha3SeekAndJoinOpTest, testBuildJoinHashInfoNoJoinField) {
    string query = "config=cluster:auction,start:0,hit:100&&query=cat_id:1001 "
                   "OR cat_id:1002 OR cat_id:1006";
    string defaultIndex = "store_id";
    _request = prepareRequest(query, _tableInfo, defaultIndex);

    initResources();
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareSeekAndJoinOp();
    vector<MatchDoc> auxMatchDocs;
    vector<int32_t> storeIds{1,1,2,3,3,4};
    std::string orignalJoinFieldName = _joinFieldName;
    _joinFieldName = "notExistFieldName";
    makeAuxMatchDocs(auxMatchDocs, storeIds);
    _joinFieldName = orignalJoinFieldName;
    auto hashJoinInfo = _seekAndJoinOp->buildJoinHashInfo(
            _auxMatchDocAllocator.get(), auxMatchDocs);
//    ASSERT_EQ(nullptr, hashJoinInfo);
}

END_HA3_NAMESPACE();
