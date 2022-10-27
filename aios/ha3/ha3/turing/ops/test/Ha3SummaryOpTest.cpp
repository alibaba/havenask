#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
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
#include <ha3_sdk/testlib/index/FakeIndexPartition.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3_sdk/testlib/index/FakeSummaryReader.h>


using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(testlib);

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


class Ha3SummaryOpTest : public Ha3OpTestBase {
private:
    Ha3SummaryOp *_summaryOp = nullptr;
    RequestPtr _request;
    TableInfoPtr _tableInfoPtr;
    IndexPartitionWrapperPtr _indexPartPtr;
    IndexPartitionReaderPtr _partitionReaderPtr;
    HitSummarySchemaPtr _hitSummarySchema;
    HitSummarySchemaPoolPtr _hitSummarySchemaPool;
    SessionMetricsCollectorPtr _collector;
    FunctionInterfaceCreatorPtr _funcCreator;
    SearcherResourcePtr _searcherResource;
    ResourceReaderPtr _resourceReaderPtr;
    SummaryProfileManagerPtr _summaryProfileManagerPtr;
    FakeSummaryExtractor *_fakeSummaryExtractor;
public:
    void SetUp() override {
        OpTestBase::SetUp();
        _summaryOp = NULL;
        _request.reset(new common::Request());
        _request.get()->setDocIdClause(new common::DocIdClause);
        _request.get()->setConfigClause(new common::ConfigClause);
        _request.get()->setQueryClause(new common::QueryClause);
        _resourceReaderPtr.reset(new ResourceReader("."));

        // init IndexPartitionWrapperPtr
        IE_NAMESPACE(partition)::FakeIndexPartitionReader *fakePartitionReader =
            new IE_NAMESPACE(partition)::FakeIndexPartitionReader();
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("mainTable"));
        fakePartitionReader->SetSchema(schema);
        fakePartitionReader->setIndexVersion(versionid_t(0));
        _partitionReaderPtr.reset(fakePartitionReader);

        FakeIndexPartition *fakeIndexPart = new FakeIndexPartition;
        fakeIndexPart->setIndexPartitionReader(_partitionReaderPtr);
        fakeIndexPart->setSchema(TEST_DATA_PATH, "/testSummaryOp/schema.json");
        IndexPartitionPtr indexPart(fakeIndexPart);

        _indexPartPtr.reset(new IndexPartitionWrapper);
        _indexPartPtr->init(indexPart);

        // init tableinfo
        _tableInfoPtr = TableInfoConfigurator().createFromFile(
                TEST_DATA_PATH "/testSummaryOp/schema.json");

        // init funcCreatorPtr
        _funcCreator.reset(new FunctionInterfaceCreator);

        // init hitSummarySchema
        _hitSummarySchema.reset(new HitSummarySchema(_tableInfoPtr.get()));

        // init hitSummarySchemaPool
        _hitSummarySchemaPool.reset(new HitSummarySchemaPool(_hitSummarySchema));

        // init summaryProfileManager
        initSummaryProfileManager();

        // init Fake index data
        makeFakeSummaryReader(fakePartitionReader,
                              "id:1001,price:5000;id:1002,price:6000;id:1003,price:7000");

        // init searcherResource
        _searcherResource.reset(new HA3_NS(service)::SearcherResource);
        _searcherResource->setIndexPartitionWrapper(_indexPartPtr);
        _searcherResource->setHitSummarySchema(_hitSummarySchema);
        _searcherResource->setHitSummarySchemaPool(_hitSummarySchemaPool);
        _searcherResource->setTableInfo(_tableInfoPtr);
        _searcherResource->setSummaryProfileManager(_summaryProfileManagerPtr);

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
        OpTestBase::TearDown();
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
        //_summaryProfileManagerPtr->setRequiredAttributeFields(_requiredAttributeFields);
    }

    void prepareSummaryOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3SummaryOp")
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
        _summaryOp = dynamic_cast<Ha3SummaryOp *>(kernel_.get());
    }

protected:
    void makeFakeSummaryReader(IE_NAMESPACE(partition)::FakeIndexPartitionReader *fakePartitionReader,
                               const std::string &summaryDocsStr)
    {
        const IE_NAMESPACE(config)::SummarySchemaPtr &summarySchemaPtr = _indexPartPtr->getSummarySchema();
        const IE_NAMESPACE(config)::FieldSchemaPtr &fieldSchemaPtr = _indexPartPtr->getFieldSchema();
        IE_NAMESPACE(index)::FakeSummaryReader *summaryReader =
            new IE_NAMESPACE(index)::FakeSummaryReader(summarySchemaPtr);
        const auto &docsStrVec = autil::StringUtil::split(summaryDocsStr, ";");
        for (auto docStr: docsStrVec) {
            summaryReader->AddSummaryDocument(prepareSummaryDocument(
                            summarySchemaPtr, fieldSchemaPtr, docStr));
        }

        fakePartitionReader->SetSummaryReader(SummaryReaderPtr(summaryReader));
    }


    SearchSummaryDocument* prepareSummaryDocument(
            const IE_NAMESPACE(config)::SummarySchemaPtr &summarySchemaPtr,
            const IE_NAMESPACE(config)::FieldSchemaPtr &fieldSchemaPtr,
            const std::string &docStr) {
        SearchSummaryDocument *ret = new SearchSummaryDocument(NULL, 8);
        const auto &fieldsStr = autil::StringUtil::split(docStr, ",");

#define SET_FIELD_VALUE(field, value)                                   \
        fieldId = fieldSchemaPtr->GetFieldId(field);                    \
        summaryFieldId = summarySchemaPtr->GetSummaryFieldId(fieldId);  \
        ret->SetFieldValue(summaryFieldId, value.data(), value.size());

        for (auto fieldStr : fieldsStr) {
            const auto &fieldKv = autil::StringUtil::split(fieldStr, ":");
            EXPECT_EQ(2u, fieldKv.size());
            fieldid_t fieldId;
            summaryfieldid_t summaryFieldId;
            SET_FIELD_VALUE(fieldKv[0], fieldKv[1]);
        }
#undef SET_FIELD_VALUE

        return ret;
    }
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
        searcherQueryResource->indexPartitionReaderWrapper.reset(
                new IndexPartitionReaderWrapper(NULL, NULL, {_partitionReaderPtr}));
    }

    Ha3RequestVariant fakeHa3Request(const std::vector<uint32_t>& docIds) {
        prepareHa3Request(docIds);
        Ha3RequestVariant variant(_request, &_pool);
        return variant;
    }

    void prepareHa3Request(const std::vector<uint32_t>& docIds) {
        common::DocIdClause *docIdClause = _request.get()->getDocIdClause();
        docIdClause->setSummaryProfileName("daogou");
        for(auto id : docIds) {
            docIdClause->addGlobalId(GlobalIdentifier(id, 0, versionid_t(0), 0, 0, 0));
        }
        _request->setDocIdClause(docIdClause);
    }

    void checkSummaryHit(Hits *hits, const std::string &expectedStr) {
        ASSERT_TRUE(hits != nullptr);
        const auto &expectedDocs = autil::StringUtil::split(expectedStr, ";");
        EXPECT_EQ(expectedDocs.size(), hits->size());
        for (size_t i = 0; i < expectedDocs.size(); i++) {
            auto expectedDocStr = expectedDocs[i];
            const auto &fieldKvStrs = autil::StringUtil::split(expectedDocStr, ",");
            for(auto fieldKv : fieldKvStrs) {
                const auto &kv = autil::StringUtil::split(fieldKv, ":");
                EXPECT_EQ(2u, kv.size());
                auto hit = hits->getHit(i);
                ASSERT_TRUE(hit != nullptr);
                hit->getSummaryHit()->setHitSummarySchema(_hitSummarySchema.get());
                EXPECT_EQ(kv[1], hit->getSummaryValue(kv[0]));
            }
        }
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3SummaryOpTest);

TEST_F(Ha3SummaryOpTest, testProcessPhaseTwoRequest) {
    {
        SetUp();
        prepareSummaryOp();
        std::vector<uint32_t> docIds;
        docIds.push_back(0);
        docIds.push_back(1);
        docIds.push_back(2);
        prepareHa3Request(docIds);
        initSearcherQueryResource();
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
        auto collector = searcherQueryResource->sessionMetricsCollector;
        ASSERT_EQ(0u, collector->getReturnCount());
        ASSERT_TRUE(false == collector->isIncreaseSearchPhase2EmptyQps());
        ResultPtr result = _summaryOp->processPhaseTwoRequest(_request.get(),
                _searcherResource, searcherQueryResource.get());
        ASSERT_TRUE(result != nullptr);
        auto hits = result->getHits();
        ASSERT_TRUE(hits != nullptr);
        ASSERT_EQ(3u, collector->getReturnCount());
        ASSERT_TRUE(false == collector->isIncreaseSearchPhase2EmptyQps());
        checkSummaryHit(hits, "id:1001,price:5000;id:1002,price:6000;id:1003,price:7000");
        TearDown();
    }

    {
        SetUp();
        prepareSummaryOp();
        std::vector<uint32_t> docIds;
        prepareHa3Request(docIds);
        initSearcherQueryResource();
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
        auto collector = searcherQueryResource->sessionMetricsCollector;
        ASSERT_EQ(0u, collector->getReturnCount());
        ASSERT_TRUE(false == collector->isIncreaseSearchPhase2EmptyQps());
        ResultPtr result = _summaryOp->processPhaseTwoRequest(_request.get(),
                _searcherResource, searcherQueryResource.get());
        ASSERT_TRUE(result != nullptr);
        auto hits = result->getHits();
        ASSERT_TRUE(hits != nullptr);
        ASSERT_EQ(0u, collector->getReturnCount());
        ASSERT_TRUE(true == collector->isIncreaseSearchPhase2EmptyQps());
        TearDown();
    }
}

TEST_F(Ha3SummaryOpTest, testProcessPhaseTwoRequestException) {
    prepareSummaryOp();

    {
        SetUp();
        std::vector<uint32_t> docIds;
        prepareHa3Request(docIds);
        _request.get()->setDocIdClause(nullptr);
        initSearcherQueryResource();
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
        auto collector = searcherQueryResource->sessionMetricsCollector;
        ASSERT_TRUE(nullptr != collector);
        EXPECT_EQ(false, collector->isIncreaseSearchPhase2EmptyQps());
        ResultPtr result = _summaryOp->processPhaseTwoRequest(_request.get(),
                _searcherResource, searcherQueryResource.get());
        EXPECT_EQ(true, collector->isIncreaseSearchPhase2EmptyQps());
        ASSERT_TRUE(result != nullptr);
        auto hits = result->getHits();
        ASSERT_TRUE(hits == nullptr);
        TearDown();
    }

    {
        SetUp();
        std::vector<uint32_t> docIds;
        prepareHa3Request(docIds);
        _request.get()->setDocIdClause(nullptr);
        initSearcherQueryResource();
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
        searcherQueryResource->sessionMetricsCollector = nullptr;
        ResultPtr result = _summaryOp->processPhaseTwoRequest(_request.get(),
                _searcherResource, searcherQueryResource.get());
        ASSERT_TRUE(result != nullptr);
        auto hits = result->getHits();
        ASSERT_TRUE(hits == nullptr);
        TearDown();
    }

}


TEST_F(Ha3SummaryOpTest, testSummaryOp) {
    prepareSummaryOp();
    initSearcherSessionResource();
    initSearcherQueryResource();
    std::vector<uint32_t> docIds;
    docIds.push_back(0);
    docIds.push_back(1);
    docIds.push_back(2);
    Variant variant = fakeHa3Request(docIds);
    AddInput<Variant>(
            TensorShape({}),
            [&variant](int x) -> Variant {
                return variant;
            });

    TF_ASSERT_OK(RunOpKernel());
    auto pOutputTensor = GetOutput(0);
    ASSERT_TRUE(pOutputTensor != nullptr);
    auto ha3ResultVariant = pOutputTensor->scalar<Variant>()().get<Ha3ResultVariant>();
    auto result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    auto hits = result->getHits();
    ASSERT_TRUE(hits != nullptr);
    checkSummaryHit(hits, "id:1001,price:5000;id:1002,price:6000;id:1003,price:7000");
}

END_HA3_NAMESPACE();
