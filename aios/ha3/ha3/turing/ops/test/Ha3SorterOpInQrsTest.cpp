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
#include <ha3/turing/ops/QrsQueryResource.h>
#include <suez/turing/expression/function/FunctionInterfaceCreator.h>
#include <ha3_sdk/testlib/index/FakeIndexPartition.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>


using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
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


class Ha3SorterOpInQrsTest : public Ha3OpTestBase {
private:
    Ha3SorterOp *_sorterOp = nullptr;

    TableInfoPtr _tableInfo;

    RequestPtr _request;

    Ha3MatchDocAllocatorPtr _matchDocAllocator;

    ResourceReaderPtr _resourceReader;
    SorterManagerPtr _sorterManager;
    ClusterSorterManagersPtr _clusterSorterManagers;
public:
    virtual tensorflow::QueryResourcePtr createQueryResource() override{
        QrsQueryResource *qrsQueryResource = new QrsQueryResource();
        tensorflow::QueryResourcePtr queryResource(qrsQueryResource);
        return queryResource;
    }

    virtual tensorflow::SessionResourcePtr createSessionResource(uint32_t count) override{
        QrsSessionResource *qrsSessionResource = new QrsSessionResource(count);
        tensorflow::SessionResourcePtr sessionResource(qrsSessionResource);
        return sessionResource;
    }

public:
    void SetUp() override {
        OpTestBase::SetUp();
        _tableInfo = TableInfoConfigurator().createFromFile(
                TEST_DATA_PATH "/testSorterOp/schema.json");
    }

    void TearDown() override {
        _tableInfo.reset();
        OpTestBase::TearDown();
    }

    void initResources() {
        // init _matchDocAllocator
        _matchDocAllocator.reset(new Ha3MatchDocAllocator(&_pool, false));

        // init _resourceReader
        _resourceReader.reset(new ResourceReader(TEST_DATA_PATH"/searcher_test"));

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

        // init _clusterSorterManagers
        _clusterSorterManagers.reset(new std::map<std::string, SorterManagerPtr>);
        _clusterSorterManagers->insert(std::make_pair("cluster1.default", _sorterManager));
    }

    void releaseResources()
    {
        _clusterSorterManagers.reset();
        _sorterManager.reset();
        _resourceReader.reset();
        _matchDocAllocator.reset();
        _sorterOp = NULL;
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

    void makeMatchDocs(vector<MatchDoc> &matchDocVec, uint32_t docNum) {
        matchDocVec.clear();

        auto matchDocAllocator = _matchDocAllocator;
        Reference<int32_t> *ref = matchDocAllocator->declare<int32_t>("id");

        for (size_t i = 0; i < docNum; i++) {
            MatchDoc doc = matchDocAllocator->allocate(i);
            matchDocVec.push_back(doc);
            ref->set(doc, i + 1);
        }
    }

protected:
    void initQrsSessionResource() {
        auto qrsSessionResource = dynamic_pointer_cast<QrsSessionResource>(
                _sessionResource);
        ASSERT_TRUE(qrsSessionResource != nullptr);

        qrsSessionResource->_clusterSorterManagers = _clusterSorterManagers;
        // TODO: add other needed dependencies
    }

    void initQrsQueryResource() {
        auto qrsQueryResource = dynamic_pointer_cast<QrsQueryResource>(
                getQueryResource());
        ASSERT_TRUE(qrsQueryResource != nullptr);

        // TODO: add other needed dependencies
    }

    Ha3RequestVariant fakeHa3Request(const std::string& query, const std::string& defaultIndex) {
        _request = prepareRequest(query, _tableInfo, defaultIndex);
        Ha3RequestVariant variant(_request, &_pool);
        return variant;
    }

    MatchDocsVariant fakeMatchDocs(std::vector<matchdoc::MatchDoc> &docs) {
        MatchDocsVariant variant(_matchDocAllocator, &_pool);
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

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3SorterOpInQrsTest);

TEST_F(Ha3SorterOpInQrsTest, testSortInQrsNotNeedSort) {
    std::string attrLocation = "SL_QRS";

    prepareSorterOp(attrLocation);

    string query = "config=cluster:cluster1&&query=company_id:1&&sort=-id";
    _request = prepareRequest(query, _tableInfo, "company_id");

    initResources();
    initQrsSessionResource();
    initQrsQueryResource();

    vector<MatchDoc> matchDocVec;
    uint32_t docNum = 3u;
    makeMatchDocs(matchDocVec, docNum);
    MatchDocsVariant matchDocsVariant = fakeMatchDocs(matchDocVec);
    EXPECT_EQ(0, matchDocVec.size());

    QueryResourcePtr queryResource = getQueryResource();
    uint32_t extraRankCount = 10;
    uint32_t totalMatchDocs = 100;
    uint32_t actualMatchDocs = 10;
    uint32_t srcCount = 1;

    bool ret = _sorterOp->sortInQrs(_request, _sessionResource.get(), queryResource.get(), &matchDocsVariant,
                                    extraRankCount, totalMatchDocs, actualMatchDocs, srcCount);

    ASSERT_TRUE(ret);

    EXPECT_EQ(100, totalMatchDocs);
    EXPECT_EQ(10, actualMatchDocs);

    matchDocsVariant.stealMatchDocs(matchDocVec);
    EXPECT_EQ(docNum, matchDocVec.size());

    checkDocId(matchDocVec, "0,1,2");

    auto matchDocAllocator = _matchDocAllocator;
    auto idRef = matchDocAllocator->findReference<int32_t>("id");
    checkAttribute(matchDocVec, idRef, "1,2,3");

    releaseResources();
}

TEST_F(Ha3SorterOpInQrsTest, testSortInQrs) {
    std::string attrLocation = "SL_QRS";

    prepareSorterOp(attrLocation);

    string query = "config=cluster:cluster1&&query=company_id:1&&sort=-id";
    _request = prepareRequest(query, _tableInfo, "company_id");

    initResources();
    initQrsSessionResource();
    initQrsQueryResource();

    vector<MatchDoc> matchDocVec;
    uint32_t docNum = 3u;
    makeMatchDocs(matchDocVec, docNum);
    MatchDocsVariant matchDocsVariant = fakeMatchDocs(matchDocVec);
    EXPECT_EQ(0, matchDocVec.size());

    QueryResourcePtr queryResource = getQueryResource();
    uint32_t extraRankCount = 10;
    uint32_t totalMatchDocs = 100;
    uint32_t actualMatchDocs = 10;
    uint32_t srcCount = 2;

    bool ret = _sorterOp->sortInQrs(_request, _sessionResource.get(), queryResource.get(), &matchDocsVariant,
                                    extraRankCount, totalMatchDocs, actualMatchDocs, srcCount);

    ASSERT_TRUE(ret);

    EXPECT_EQ(100, totalMatchDocs);
    EXPECT_EQ(10, actualMatchDocs);

    matchDocsVariant.stealMatchDocs(matchDocVec);
    EXPECT_EQ(docNum, matchDocVec.size());

    checkDocId(matchDocVec, "2,1,0");

    auto matchDocAllocator = _matchDocAllocator;
    auto idRef = matchDocAllocator->findReference<int32_t>("id");
    checkAttribute(matchDocVec, idRef, "3,2,1");

    releaseResources();
}

TEST_F(Ha3SorterOpInQrsTest, testCreateSorter) {
    {
        std::string attrLocation = "SL_QRS";

        prepareSorterOp(attrLocation);

        string query = "config=cluster:cluster1&&query=company_id:1&&sort=-id";
        _request = prepareRequest(query, _tableInfo, "company_id");

        initResources();
        initQrsSessionResource();
        initQrsQueryResource();

        ClusterSorterManagersPtr emptyClusterSorterManagers;
        auto pSorter = _sorterOp->createSorterWrapper(_request.get(),
                emptyClusterSorterManagers, &_pool);
        ASSERT_TRUE(pSorter == nullptr);

        releaseResources();
    }

    {
        std::string attrLocation = "SL_QRS";

        prepareSorterOp(attrLocation);

        string query = "config=cluster:cluster1&&query=company_id:1&&sort=-id";
        _request = prepareRequest(query, _tableInfo, "company_id");

        initResources();
        initQrsSessionResource();
        initQrsQueryResource();

        _clusterSorterManagers->erase("cluster1.default");

        auto pSorter = _sorterOp->createSorterWrapper(_request.get(),
                _clusterSorterManagers, &_pool);
        ASSERT_TRUE(pSorter == nullptr);

        releaseResources();
    }

    {
        std::string attrLocation = "SL_QRS";

        prepareSorterOp(attrLocation);

        string query = "config=cluster:cluster1&&query=company_id:1&&sort=-id";
        _request = prepareRequest(query, _tableInfo, "company_id");

        initResources();
        initQrsSessionResource();
        initQrsQueryResource();

        auto pSorter = _sorterOp->createSorterWrapper(_request.get(),
                _clusterSorterManagers, &_pool);
        ASSERT_TRUE(pSorter != nullptr);
        POOL_DELETE_CLASS(pSorter);
        pSorter = NULL;

        releaseResources();
    }
}

TEST_F(Ha3SorterOpInQrsTest, testOpSimple) {
    std::string attrLocation = "SL_QRS";
    prepareSorterOp(attrLocation);

    string query = "config=cluster:cluster1,start:0,hit:100&&query=company_id:1&&sort=-id";
    string defaultIndex = "company_id";
    Ha3RequestVariant requestVariant = fakeHa3Request(query, defaultIndex);

    initResources();
    initQrsSessionResource();
    initQrsQueryResource();

    auto matchDocAllocator = _matchDocAllocator;
    vector<MatchDoc> matchDocVec;
    uint32_t docNum = 3u;
    makeMatchDocs(matchDocVec, docNum);
    MatchDocsVariant matchDocsVariant = fakeMatchDocs(matchDocVec);
    EXPECT_EQ(0, matchDocVec.size());


    uint32_t extraRankCount = 10;
    uint32_t totalMatchDocs = 20;
    uint32_t actualMatchDocs = 20;
    uint16_t srcCount = 2;

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
