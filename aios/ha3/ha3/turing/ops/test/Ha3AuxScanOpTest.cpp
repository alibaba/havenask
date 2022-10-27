#include <suez/turing/common/JoinConfigInfo.h>
#include <tensorflow/core/lib/core/error_codes.pb.h>
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
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>
#include <ha3/turing/ops/Ha3AuxScanOp.h>
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
#include <indexlib/test/schema_maker.h>
#include <indexlib/test/partition_state_machine.h>
#include <indexlib/config/index_partition_options.h>
#include <indexlib/partition/partition_reader_snapshot.h>
#include <indexlib/partition/index_application.h>
#include <ha3/search/IndexPartitionReaderUtil.h>

using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(func_expression);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(qrs);

BEGIN_HA3_NAMESPACE(turing);


class Ha3AuxScanOpTest : public Ha3OpTestBase {
public:
    Ha3AuxScanOpTest() {
        _searcherSessionResource = nullptr;
        _indexRoot = GET_TEMP_DATA_PATH() + "testAuxScanOp";
	_tableName = "company";
	_joinFieldName = "company_id";
    }

    void SetUp() override {
        OpTestBase::SetUp();
        fslib::fs::FileSystem::remove(_indexRoot);
        initResources();
    }

    void TearDown() override {
        OpTestBase::TearDown();
        releaseResources();
    }

    void initResources() {
        IndexPartitionOptions options;
        string zoneName = _tableName;
        string field = "id:uint64:pk;company_id:int32;cat_id:int32;price:int32";
        string index = "pk:primarykey64:id;company_id:number:company_id;";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
                field, index, "company_id;cat_id;price", "");
        schema->SetSchemaName(zoneName);

	_psm.reset(new PartitionStateMachine);
        ASSERT_TRUE(_psm->Init(schema, options, _indexRoot));

        string fullDocs0 = "cmd=add,id=0,company_id=1,cat_id=1001,price=100;"
	    "cmd=add,id=1,company_id=2,cat_id=1002,price=200;"
	    "cmd=add,id=2,company_id=2,cat_id=1003,price=300;"
	    "cmd=add,id=3,company_id=3,cat_id=1004,price=400;"
	    "cmd=add,id=4,company_id=3,cat_id=1005,price=500;"
	    "cmd=add,id=5,company_id=4,cat_id=1006,price=600;"
	    ;
        ASSERT_TRUE(_psm->Transfer(BUILD_FULL, fullDocs0, "", ""));
        IE_NAMESPACE(partition)::IndexPartitionPtr indexPart = _psm->GetIndexPartition();
        _indexApp.reset(new IE_NAMESPACE(partition)::IndexApplication());
        ASSERT_TRUE(_indexApp->Init({ indexPart }, IE_NAMESPACE(partition)::JoinRelationMap()));
        _snapshotPtr = _indexApp->CreateSnapshot();
        _tableInfo = TableInfoConfigurator::createFromIndexApp(_tableName,
	    _indexApp);

        _auxScanOp = NULL;

        _searcherResource.reset(new SearcherResource);
        _searcherResource->setTableInfo(_tableInfo);
	FunctionInterfaceCreatorPtr funcCreator(new FunctionInterfaceCreator);
	_searcherResource->setFunctionCreator(funcCreator);
	ClusterConfigInfo clusterConfig;
        clusterConfig._joinConfig._scanJoinCluster = _tableName;
        JoinConfigInfo joinConfigInfo(
            true, _tableName, _joinFieldName, false, false);
        clusterConfig._joinConfig.addJoinInfo(joinConfigInfo);
        _searcherResource->_clusterConfig = clusterConfig;
    }

    void releaseResources()
    {
	_auxScanOp = NULL;
	_request.reset();
        if (_searcherSessionResource) {
            _searcherSessionResource->searcherResource.reset();
        }
	_searcherResource.reset();
        _tableInfo.reset();
	_snapshotPtr.reset();
	_indexApp.reset();
	_psm.reset();
        fslib::fs::FileSystem::remove(_indexRoot);
    }

    RequestPtr prepareRequest(const string &query, bool validateFilter=true)
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(query);
        if (!requestPtr) {
            return requestPtr;
        }
        requestPtr->setPool(&_pool);

        const FilterClause *auxFilterClause = requestPtr->getAuxFilterClause();
	if (auxFilterClause && validateFilter) {
            SyntaxExprValidator syntaxExprValidator(
                    _tableInfo->getAttributeInfos(), {}, false);
            if (!syntaxExprValidator.validate(
                        auxFilterClause->getRootSyntaxExpr())) {
                return RequestPtr();
	    }
        }
        return requestPtr;
    }

    void prepareAuxScanOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3AuxScanOp")
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
        _auxScanOp = dynamic_cast<Ha3AuxScanOp *>(kernel_.get());
    }

protected:
    void initSearcherSessionResource() {
	_sessionResource->dependencyTableInfoMap[_tableName] = _tableInfo;
        auto searcherSessionResource =
                dynamic_pointer_cast<SearcherSessionResource>(_sessionResource);
        ASSERT_TRUE(searcherSessionResource != nullptr);
        searcherSessionResource->searcherResource = _searcherResource;
        _searcherSessionResource = searcherSessionResource.get();
    }

    void initSearcherQueryResource() {
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(
                getQueryResource());
        ASSERT_TRUE(searcherQueryResource != nullptr);
        searcherQueryResource->setPool(&_pool);
	searcherQueryResource->setIndexSnapshot(_snapshotPtr);
    }

    Ha3RequestVariant fakeHa3Request(
        const std::string &query, bool validateFilter = true) {
        _request = prepareRequest(query, validateFilter);
        Ha3RequestVariant variant(_request, &_pool);
        return variant;
    }

    AttributeExpressionCreatorPtr createAttributeExpressionCreator(
        const MatchDocAllocatorPtr &matchDocAllocator) {
        return AttributeExpressionCreatorPtr( new AttributeExpressionCreator(
            &_pool,
            matchDocAllocator.get(),
            _tableName,
            _snapshotPtr.get(),
            _tableInfo,
            VirtualAttributes(),
            _searcherResource->getFuncCreator().get(),
            _sessionResource->cavaPluginManager,
            nullptr));
    }

private:
    string _indexRoot;
    string _tableName;
    string _joinFieldName;
    SearcherSessionResource *_searcherSessionResource;
    PartitionStateMachinePtr _psm;
    IE_NAMESPACE(partition)::IndexApplicationPtr _indexApp;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    TableInfoPtr _tableInfo;

    Ha3AuxScanOp *_auxScanOp = nullptr;
    RequestPtr _request;
    SearcherResourcePtr _searcherResource;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3AuxScanOpTest);

TEST_F(Ha3AuxScanOpTest, testOpSimple) {
    string auxQuery = "aux_query=company_id:2";
    Variant requestVariant = fakeHa3Request(auxQuery);
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareAuxScanOp();

    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant = pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    vector<MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(2u, matchDocVec.size());
    ASSERT_EQ(1, matchDocVec[0].getDocId());
    ASSERT_EQ(2, matchDocVec[1].getDocId());
    auto matchDocAllocator = matchDocsVariant->getAllocator();
    ASSERT_TRUE(matchDocAllocator.get());
    matchdoc::Reference<int32_t> *companyIdRef =
            matchDocAllocator->findReference<int32_t>(_joinFieldName);
    ASSERT_EQ(2,companyIdRef->get(matchDocVec[0]));
    ASSERT_EQ(2,companyIdRef->get(matchDocVec[1]));
}

TEST_F(Ha3AuxScanOpTest, testOpConstructFail1) {
    TF_ASSERT_OK(NodeDefBuilder("myop", "Ha3AuxScanOp")
                     .Input(FakeInput(DT_VARIANT))
                     .Finalize(node_def()));
    ASSERT_EQ(error::UNAVAILABLE, InitOp().code());
}

TEST_F(Ha3AuxScanOpTest, testOpConstructFail2) {
    initSearcherSessionResource();
    _searcherResource->_clusterConfig._joinConfig._scanJoinCluster = "";
    TF_ASSERT_OK(NodeDefBuilder("myop", "Ha3AuxScanOp")
                     .Input(FakeInput(DT_VARIANT))
                     .Finalize(node_def()));
    ASSERT_EQ(error::UNAVAILABLE, InitOp().code());
}

TEST_F(Ha3AuxScanOpTest, testOpConstructFail3) {
    initSearcherSessionResource();
    _searcherResource->_clusterConfig._joinConfig.getJoinInfos().clear();
    TF_ASSERT_OK(NodeDefBuilder("myop", "Ha3AuxScanOp")
                     .Input(FakeInput(DT_VARIANT))
                     .Finalize(node_def()));
    ASSERT_EQ(error::UNAVAILABLE, InitOp().code());
}

TEST_F(Ha3AuxScanOpTest, testOpConstructFail4) {
    initSearcherSessionResource();
    auto &joinInfos =
        _searcherResource->_clusterConfig._joinConfig.getJoinInfos();
    ASSERT_EQ(1u, joinInfos.size());
    joinInfos[0].joinCluster.clear();
    TF_ASSERT_OK(NodeDefBuilder("myop", "Ha3AuxScanOp")
                     .Input(FakeInput(DT_VARIANT))
                     .Finalize(node_def()));
    ASSERT_EQ(error::UNAVAILABLE, InitOp().code());
}

TEST_F(Ha3AuxScanOpTest, testOpConstructFail5) {
    initSearcherSessionResource();
    auto &joinInfos =
        _searcherResource->_clusterConfig._joinConfig.getJoinInfos();
    ASSERT_EQ(1u, joinInfos.size());
    joinInfos[0].joinField.clear();
    TF_ASSERT_OK(NodeDefBuilder("myop", "Ha3AuxScanOp")
                     .Input(FakeInput(DT_VARIANT))
                     .Finalize(node_def()));
    ASSERT_EQ(error::UNAVAILABLE, InitOp().code());
}

TEST_F(Ha3AuxScanOpTest, testQueryAndFilter) {
    string auxQuery = "aux_query=company_id:2&&aux_filter=price>200";
    Variant requestVariant = fakeHa3Request(auxQuery);

    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareAuxScanOp();

    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant =
            pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    vector<MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(1u, matchDocVec.size());
    ASSERT_EQ(2, matchDocVec[0].getDocId());
    auto matchDocAllocator = matchDocsVariant->getAllocator();
    ASSERT_TRUE(matchDocAllocator.get());
    matchdoc::Reference<int32_t> *companyIdRef =
            matchDocAllocator->findReference<int32_t>(_joinFieldName);
    ASSERT_EQ(2,companyIdRef->get(matchDocVec[0]));
}

TEST_F(Ha3AuxScanOpTest, testQueryWithWrongFilter) {
    string auxQuery = "aux_query=company_id:2&&aux_filter=price_not_exist>200";
    Variant requestVariant = fakeHa3Request(auxQuery);
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareAuxScanOp();
    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });
    ASSERT_NE(::tensorflow::Status::OK(), RunOpKernel());
}

TEST_F(Ha3AuxScanOpTest, testQueryWithWrongFilterWithoutValidateFilter) {
    string auxQuery = "aux_query=company_id:2&&aux_filter=price_not_exist>200";
    Variant requestVariant = fakeHa3Request(auxQuery, false);
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareAuxScanOp();
    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant = pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    vector<MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(2u, matchDocVec.size());
    ASSERT_EQ(1, matchDocVec[0].getDocId());
    ASSERT_EQ(2, matchDocVec[1].getDocId());
    auto matchDocAllocator = matchDocsVariant->getAllocator();
    ASSERT_TRUE(matchDocAllocator.get());
    matchdoc::Reference<int32_t> *companyIdRef =
            matchDocAllocator->findReference<int32_t>(_joinFieldName);
    ASSERT_EQ(2,companyIdRef->get(matchDocVec[0]));
    ASSERT_EQ(2,companyIdRef->get(matchDocVec[1]));
}

TEST_F(Ha3AuxScanOpTest, testEmptyQueryAndFilter) {
    string auxQuery = "";
    Variant requestVariant = fakeHa3Request(auxQuery);
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareAuxScanOp();

    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant =
            pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    vector<MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(0, matchDocVec.size());
    auto matchDocAllocator = matchDocsVariant->getAllocator();
    ASSERT_FALSE(matchDocAllocator.get());
}

TEST_F(Ha3AuxScanOpTest, testEmptyQueryButWithFilter) {
    string auxQuery = "aux_filter=price>200";
    Variant requestVariant = fakeHa3Request(auxQuery);
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareAuxScanOp();

    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant =
            pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    vector<MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(4u, matchDocVec.size());
    ASSERT_EQ(2, matchDocVec[0].getDocId());
    ASSERT_EQ(3, matchDocVec[1].getDocId());
    ASSERT_EQ(4, matchDocVec[2].getDocId());
    ASSERT_EQ(5, matchDocVec[3].getDocId());
    auto matchDocAllocator = matchDocsVariant->getAllocator();
    ASSERT_TRUE(matchDocAllocator.get());
    matchdoc::Reference<int32_t> *companyIdRef =
            matchDocAllocator->findReference<int32_t>(_joinFieldName);
    ASSERT_EQ(2,companyIdRef->get(matchDocVec[0]));
    ASSERT_EQ(3,companyIdRef->get(matchDocVec[1]));
    ASSERT_EQ(3,companyIdRef->get(matchDocVec[2]));
    ASSERT_EQ(4,companyIdRef->get(matchDocVec[3]));
}

TEST_F(Ha3AuxScanOpTest, testNoMatchQuery) {
    string auxQuery = "aux_query=company_id:1000";
    Variant requestVariant = fakeHa3Request(auxQuery);
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareAuxScanOp();

    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant =
            pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    vector<MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(0, matchDocVec.size());
}

TEST_F(Ha3AuxScanOpTest, testNoMatchQueryWithFilter) {
    string auxQuery = "aux_query=company_id:1000&&aux_filter=price>200";
    Variant requestVariant = fakeHa3Request(auxQuery);
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareAuxScanOp();

    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant =
            pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    vector<MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(0, matchDocVec.size());
}

TEST_F(Ha3AuxScanOpTest, testFilterNoMatch) {
    string auxQuery = "aux_query=company_id:2&&aux_filter=price>200000";
    Variant requestVariant = fakeHa3Request(auxQuery);
    initSearcherSessionResource();
    initSearcherQueryResource();

    prepareAuxScanOp();

    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });

    TF_ASSERT_OK(RunOpKernel());

    auto pMatchDocsOutputTensor = GetOutput(0);
    ASSERT_TRUE(pMatchDocsOutputTensor != nullptr);
    auto matchDocsVariant =
            pMatchDocsOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);
    vector<MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(0, matchDocVec.size());
}

TEST_F(Ha3AuxScanOpTest, testCreateFilterWrapper) {
    initSearcherSessionResource();
    prepareAuxScanOp();
    MatchDocAllocatorPtr matchDocAllocator{ new MatchDocAllocator(
        &_pool, false) };
    AttributeExpressionCreatorPtr attrExprCreator =
        createAttributeExpressionCreator(matchDocAllocator);
    {
        FilterWrapperPtr filterWrapper = _auxScanOp->createFilterWrapper(
            nullptr, attrExprCreator.get(), matchDocAllocator.get(), &_pool);
        ASSERT_EQ(nullptr, filterWrapper.get());
    }

    {
        string auxQuery = "aux_query=company_id:2&&aux_filter=price>200000";
        RequestPtr request = prepareRequest(auxQuery);
        FilterWrapperPtr filterWrapper =
            _auxScanOp->createFilterWrapper(request->getAuxFilterClause(),
                attrExprCreator.get(),
                matchDocAllocator.get(),
                &_pool);
        ASSERT_NE(nullptr, filterWrapper.get());
    }

    {
        string auxQuery = "aux_query=company_id:2&&aux_filter=price11>200000";
        RequestPtr request = prepareRequest(auxQuery, false);
        FilterWrapperPtr filterWrapper =
            _auxScanOp->createFilterWrapper(request->getAuxFilterClause(),
                attrExprCreator.get(),
                matchDocAllocator.get(),
                &_pool);
        ASSERT_EQ(nullptr, filterWrapper.get());
    }

    {
        MatchDocAllocatorPtr matchDocAllocator{ new MatchDocAllocator(
            &_pool, true) };
        AttributeExpressionCreatorPtr attrExprCreator =
            createAttributeExpressionCreator(matchDocAllocator);
        {
            FilterWrapperPtr filterWrapper =
                _auxScanOp->createFilterWrapper(nullptr,
                    attrExprCreator.get(),
                    matchDocAllocator.get(),
                    &_pool);
            ASSERT_NE(nullptr, filterWrapper.get());
	    ASSERT_NE(nullptr, filterWrapper->getSubDocFilter());
        }
    }
}

TEST_F(Ha3AuxScanOpTest, testCreateLayerMeta) {
    initSearcherSessionResource();
    prepareAuxScanOp();

    auto indexPartitionReaderWrapper =
        IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
                _snapshotPtr.get(), _tableName);
    LayerMetaPtr layerMeta = _auxScanOp->createLayerMeta(indexPartitionReaderWrapper, &_pool);
    ASSERT_TRUE(layerMeta);
    ASSERT_EQ(1u, layerMeta->size());
    const auto &rangeMeta = (*layerMeta)[0];
    ASSERT_EQ(0, rangeMeta.begin);
    ASSERT_EQ(5, rangeMeta.end);
    ASSERT_EQ(0, rangeMeta.nextBegin);
    ASSERT_EQ(rangeMeta.quota, rangeMeta.end - rangeMeta.begin + 1);
}

END_HA3_NAMESPACE();
