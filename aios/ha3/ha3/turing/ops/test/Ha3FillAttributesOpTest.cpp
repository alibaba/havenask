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
#include <ha3/turing/ops/Ha3FillAttributesOp.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>

using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(turing);


class Ha3FillAttributesOpTest : public Ha3OpTestBase {
private:
    Ha3FillAttributesOp *_fillAttributesOp = nullptr;
    common::RequestPtr _request;
    IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    TableInfoPtr _tableInfoPtr;
    SearchCommonResourcePtr _commonResource;
    SearchPartitionResourcePtr _searchPartitionResource;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    IE_NAMESPACE(partition)::TableMem2IdMap _emptyTableMem2IdMap;
    AttributeExpressionCreatorPtr _attrExprCreator;
public:
    void SetUp() override {
        OpTestBase::SetUp();
        _snapshotPtr.reset(new PartitionReaderSnapshot(&_emptyTableMem2IdMap,
                        &_emptyTableMem2IdMap, &_emptyTableMem2IdMap,
                        std::vector<IndexPartitionReaderInfo>()));
        _fillAttributesOp = NULL;
        _request.reset(new common::Request());
        _request.get()->setConfigClause(new common::ConfigClause);
        _request.get()->setAttributeClause(new common::AttributeClause);

        FakeIndex fakeIndex;
        fakeIndex.versionId = 1;
        fakeIndex.indexes["pk"] = "1:1,2:2,3:3,4:4,5:5,6:6;\n";
        fakeIndex.attributes = "id : int32_t : 1, 2, 3, 4, 5, 6;"
                               "company_id : int32_t : 1, 1, 1, 2, 2, 2;"
                               "cat_id : int32_t : 1, 2, 6, 3, 4, 5;";
        _indexPartitionReaderWrapper = 
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);

        _tableInfoPtr = TableInfoConfigurator().createFromFile(
                TEST_DATA_PATH "/testFillAttributesOp/schema.json");
        std::map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
        _commonResource.reset(new SearchCommonResource(&_pool, _tableInfoPtr, 
                        nullptr, nullptr, nullptr, nullptr, CavaPluginManagerPtr(),
                        _request.get(), nullptr, cavaJitModules));
        _commonResource->matchDocAllocator->setDefaultGroupName(string());
        _searchPartitionResource.reset(
            new SearchPartitionResource(*_commonResource, _request.get(),
                    _indexPartitionReaderWrapper, _snapshotPtr));
        _attrExprCreator.reset(new FakeAttributeExpressionCreator(&_pool, 
                        _indexPartitionReaderWrapper, NULL, NULL, NULL, NULL, NULL));
        _searchPartitionResource->attributeExpressionCreator = _attrExprCreator;
    }

    void TearDown() override {
        _snapshotPtr.reset();
        _attrExprCreator.reset();
        _searchPartitionResource.reset();
        _commonResource.reset();
        _tableInfoPtr.reset();
        _indexPartitionReaderWrapper.reset();
        _request.reset();
        _pool.reset();
        _fillAttributesOp = NULL;
        OpTestBase::TearDown();
    }

    void prepareFillAttributeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3FillAttributesOp")
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
        _fillAttributesOp = dynamic_cast<Ha3FillAttributesOp *>(kernel_.get());
    }

protected:
    void initSearcherQueryResource() {
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(
                getQueryResource());
        ASSERT_TRUE(searcherQueryResource != nullptr);
        searcherQueryResource->partitionResource = _searchPartitionResource;
        searcherQueryResource->commonResource = _commonResource;
    }


    Ha3RequestVariant fakeHa3Request(uint8_t phaseOneBasicInfoMask,
            uint32_t fetchSummaryType,
            const vector<string> &attributes) {
        common::ConfigClause *configClause = _request.get()->getConfigClause();
        configClause->setPhaseOneBasicInfoMask(phaseOneBasicInfoMask);
        configClause->setFetchSummaryType(fetchSummaryType);

        common::AttributeClause *attributeClause = _request.get()->getAttributeClause();
        attributeClause->setAttributeNames(attributes.begin(), attributes.end());

        Ha3RequestVariant variant(_request, &_pool);
        return variant;
    }

    MatchDocsVariant fakeMatchDocs(std::vector<matchdoc::MatchDoc> docs) {
        auto matchDocAllocator = _commonResource->matchDocAllocator;
        MatchDocsVariant variant(matchDocAllocator, &_pool);
        variant.stealMatchDocs(docs);
        return variant;
    }

    template<typename T>
    void checkAttribute(const std::vector<matchdoc::MatchDoc> &matchDocVec, 
                        matchdoc::Reference<T> *ref, 
                        const std::string& expectedStr) {
        ASSERT_TRUE(ref != nullptr);
        vector<string> expectedAttrVec = autil::StringUtil::split(expectedStr, ",");
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

HA3_LOG_SETUP(turing, Ha3FillAttributesOpTest);

TEST_F(Ha3FillAttributesOpTest, testFillAttribute) {

    prepareFillAttributeOp();

    common::AttributeClause *attributeClause = _request.get()->getAttributeClause();
    attributeClause->addAttribute("id");
    attributeClause->addAttribute("company_id");
    attributeClause->addAttribute("cat_id");

    std::vector<matchdoc::MatchDoc> matchDocVec;
    common::Ha3MatchDocAllocatorPtr matchDocAllocator = _commonResource->matchDocAllocator;

    for (size_t i=0; i<6; i++) {
        matchDocVec.push_back(matchDocAllocator->allocate(i));
    }

    AttributeExpressionCreator &attributeExpressionCreator = 
        *_searchPartitionResource->attributeExpressionCreator;

    bool ret = _fillAttributesOp->fillAttribute(attributeClause, matchDocVec, matchDocAllocator,
                              attributeExpressionCreator);
    ASSERT_TRUE(ret);

    auto idRef = matchDocAllocator->findReference<int32_t>("id");
    ASSERT_TRUE(idRef != nullptr);
    checkAttribute(matchDocVec, idRef, "1,2,3,4,5,6");

    auto companyIdRef = matchDocAllocator->findReference<int32_t>("company_id");
    ASSERT_TRUE(companyIdRef != nullptr);
    checkAttribute(matchDocVec, companyIdRef, "1,1,1,2,2,2");

    auto catIdRef = matchDocAllocator->findReference<int32_t>("cat_id");
    ASSERT_TRUE(catIdRef != nullptr);
    checkAttribute(matchDocVec, catIdRef, "1,2,6,3,4,5");
}

TEST_F(Ha3FillAttributesOpTest, testFillAttributeException) {

    prepareFillAttributeOp();

    common::AttributeClause *attributeClause = _request.get()->getAttributeClause();
    attributeClause->addAttribute("id");
    attributeClause->addAttribute("company_id");
    attributeClause->addAttribute("fake_id");

    std::vector<matchdoc::MatchDoc> matchDocVec;
    common::Ha3MatchDocAllocatorPtr matchDocAllocator = _commonResource->matchDocAllocator;

    AttributeExpressionCreator &attributeExpressionCreator = 
        *_searchPartitionResource->attributeExpressionCreator;

    uint32_t docSizeBefore = matchDocAllocator->getDocSize();

    bool ret = _fillAttributesOp->fillAttribute(attributeClause, matchDocVec, matchDocAllocator,
                              attributeExpressionCreator);
    ASSERT_FALSE(ret);

    uint32_t docSizeAfter = matchDocAllocator->getDocSize();

    ASSERT_EQ(docSizeBefore, docSizeAfter);
}

TEST_F(Ha3FillAttributesOpTest, testFillPKNormal) {

    prepareFillAttributeOp();

    common::ConfigClause *configClause = _request.get()->getConfigClause();
    uint8_t phaseOneInfoMask = 0;
    phaseOneInfoMask |= common::pob_rawpk_flag;
    configClause->setPhaseOneBasicInfoMask(phaseOneInfoMask);
    configClause->setFetchSummaryType(BY_DOCID);

    std::vector<matchdoc::MatchDoc> matchDocVec;
    common::Ha3MatchDocAllocatorPtr matchDocAllocator = _commonResource->matchDocAllocator;

    for (size_t i=0; i<6; i++) {
        matchDocVec.push_back(matchDocAllocator->allocate(i));
    }

    AttributeExpressionCreator &attributeExpressionCreator = 
        *_searchPartitionResource->attributeExpressionCreator;

    string exprName;
    string refName;

    _fillAttributesOp->getPKExprInfo(phaseOneInfoMask, exprName, refName, _commonResource);
    ASSERT_EQ(string("id"), exprName);
    ASSERT_EQ(RAW_PRIMARYKEY_REF, refName);
    bool ret = _fillAttributesOp->fillPk(configClause, matchDocVec, matchDocAllocator,
                              attributeExpressionCreator, _commonResource);
    auto pkRefBase = matchDocAllocator->findReferenceByName(refName);
    ASSERT_TRUE(pkRefBase != nullptr);    
    auto pkRef = matchDocAllocator->findReference<int32_t>(refName);
    ASSERT_TRUE(pkRef != nullptr);
    for (size_t i = 0; i < 6; i++) {
        auto pkVal = pkRef->get(matchDocVec[i]);
        EXPECT_EQ(i+1, pkVal);
    }
    ASSERT_TRUE(ret);
}

TEST_F(Ha3FillAttributesOpTest, testFillPKException) {

    prepareFillAttributeOp();

    // return @ (!getPKExprInfo...)
    {
        std::map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
        TableInfoPtr tableInfoPtr = TableInfoConfigurator().createFromFile(
                TEST_DATA_PATH "/testFillAttributesOp/schema_without_primary_info.json");
        SearchCommonResourcePtr commonResource(new SearchCommonResource(nullptr, tableInfoPtr, 
                        nullptr, nullptr, nullptr, nullptr, CavaPluginManagerPtr(), _request.get(), nullptr, cavaJitModules));
        common::ConfigClause *configClause = _request.get()->getConfigClause();
        uint8_t phaseOneInfoMask = 0;
        phaseOneInfoMask |= common::pob_rawpk_flag;
        configClause->setPhaseOneBasicInfoMask(phaseOneInfoMask);
        configClause->setFetchSummaryType(BY_DOCID);

        AttributeExpressionCreator &attributeExpressionCreator = 
            *_searchPartitionResource->attributeExpressionCreator;
        auto matchDocAllocator = commonResource->matchDocAllocator;
        vector<MatchDoc> matchDocVec;
        bool ret = _fillAttributesOp->fillPk(configClause, matchDocVec, matchDocAllocator,
                attributeExpressionCreator, commonResource);
        ASSERT_FALSE(ret);
    }

    // return @ (!isPKNecessary && !pkExpr)
    {
        std::map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
        TableInfoPtr tableInfoPtr = TableInfoConfigurator().createFromFile(
                TEST_DATA_PATH "/testFillAttributesOp/schema_with_not_existed_pk.json");
        SearchCommonResourcePtr commonResource(new SearchCommonResource(nullptr, tableInfoPtr, 
                        nullptr, nullptr, nullptr, nullptr, CavaPluginManagerPtr(), _request.get(), nullptr, cavaJitModules));
        common::ConfigClause *configClause = _request.get()->getConfigClause();
        uint8_t phaseOneInfoMask = 0;
        phaseOneInfoMask |= common::pob_rawpk_flag;
        configClause->setPhaseOneBasicInfoMask(phaseOneInfoMask);
        configClause->setFetchSummaryType(BY_DOCID);

        AttributeExpressionCreator &attributeExpressionCreator = 
            *_searchPartitionResource->attributeExpressionCreator;
        auto matchDocAllocator = commonResource->matchDocAllocator;

        uint32_t docSizeBefore = matchDocAllocator->getDocSize();

        vector<MatchDoc> matchDocVec;
        bool ret = _fillAttributesOp->fillPk(configClause, matchDocVec, matchDocAllocator,
                attributeExpressionCreator, commonResource);

        ASSERT_TRUE(ret);
        
        uint32_t docSizeAfter = matchDocAllocator->getDocSize();

        ASSERT_EQ(docSizeBefore, docSizeAfter);
    }

    // return @ (!pkExpr)
    {
        std::map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
        TableInfoPtr tableInfoPtr = TableInfoConfigurator().createFromFile(
                TEST_DATA_PATH "/testFillAttributesOp/schema_with_not_existed_pk.json");
        SearchCommonResourcePtr commonResource(new SearchCommonResource(nullptr, tableInfoPtr, 
                        nullptr, nullptr, nullptr, nullptr, CavaPluginManagerPtr(),
                        _request.get(), nullptr, cavaJitModules));
        common::ConfigClause *configClause = _request.get()->getConfigClause();
        uint8_t phaseOneInfoMask = 0;
        phaseOneInfoMask |= common::pob_rawpk_flag;
        configClause->setPhaseOneBasicInfoMask(phaseOneInfoMask);
        configClause->setFetchSummaryType(BY_RAWPK);

        AttributeExpressionCreator &attributeExpressionCreator = 
            *_searchPartitionResource->attributeExpressionCreator;
        auto matchDocAllocator = commonResource->matchDocAllocator;

        uint32_t docSizeBefore = matchDocAllocator->getDocSize();

        vector<MatchDoc> matchDocVec;
        bool ret = _fillAttributesOp->fillPk(configClause, matchDocVec, matchDocAllocator,
                attributeExpressionCreator, commonResource);

        ASSERT_FALSE(ret);
        
        uint32_t docSizeAfter = matchDocAllocator->getDocSize();

        ASSERT_EQ(docSizeBefore, docSizeAfter);
    }

    // return @ (!pkExpr->allocate...)
    // {
    //     std::map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
    //     TableInfoPtr tableInfoPtr = TableInfoConfigurator().createFromFile(
    //             TEST_DATA_PATH "/testFillAttributesOp/schema.json");
    //     SearchCommonResourcePtr commonResource(new SearchCommonResource(nullptr, tableInfoPtr, 
    //                     nullptr, nullptr, nullptr, nullptr, _request.get(), nullptr, cavaJitModules));
    //     common::ConfigClause *configClause = _request.get()->getConfigClause();
    //     uint8_t phaseOneInfoMask = 0;
    //     phaseOneInfoMask |= common::pob_rawpk_flag;
    //     configClause->setPhaseOneBasicInfoMask(phaseOneInfoMask);
    //     configClause->setFetchSummaryType(BY_DOCID);

    //     AttributeExpressionCreator &attributeExpressionCreator = 
    //         _searchPartitionResource->attributeExpressionCreator;
    //     auto matchDocAllocator = commonResource->matchDocAllocator;

    //     // 利用dynamic_cast转换失败：MatchDocAllocator.h, line 280
    //     matchDocAllocator->setDefaultGroupName("default_group");
    //     matchDocAllocator->declare<bool>("fake_group", "id");
    //     matchDocAllocator->extend();

    //     vector<MatchDoc> matchDocVec;
    //     bool ret = _fillAttributesOp->fillPk(configClause, matchDocVec, matchDocAllocator,
    //             attributeExpressionCreator, commonResource);
    //     ASSERT_FALSE(ret);

    // }

}

TEST_F(Ha3FillAttributesOpTest, testGetPKExprInfo) {

    std::map<size_t, ::cava::CavaJitModulePtr> cavaJitModules;
    prepareFillAttributeOp();
    {
        TableInfoPtr tableInfoPtr = TableInfoConfigurator().createFromFile(
                TEST_DATA_PATH "/testFillAttributesOp/schema.json");
        SearchCommonResourcePtr commonResource(new SearchCommonResource(nullptr, tableInfoPtr, 
                        nullptr, nullptr, nullptr, nullptr,
                        CavaPluginManagerPtr(), _request.get(), nullptr, cavaJitModules));

        uint8_t phaseOneInfoMask = 0;
        std::string exprName;
        std::string refName;
        bool ret = _fillAttributesOp->getPKExprInfo(
                phaseOneInfoMask, exprName, refName, commonResource);        
        ASSERT_TRUE(ret);
        ASSERT_EQ(exprName, PRIMARYKEY_REF);
        ASSERT_EQ(refName, PRIMARYKEY_REF);
    }

    {
        TableInfoPtr tableInfoPtr = TableInfoConfigurator().createFromFile(
                TEST_DATA_PATH "/testFillAttributesOp/schema.json");
        SearchCommonResourcePtr commonResource(new SearchCommonResource(nullptr, tableInfoPtr, 
                        nullptr, nullptr, nullptr, nullptr,
                        CavaPluginManagerPtr(), _request.get(), nullptr, cavaJitModules));

        uint8_t phaseOneInfoMask = 0;
        phaseOneInfoMask |= common::pob_rawpk_flag;
        std::string exprName;
        std::string refName;

        bool ret = _fillAttributesOp->getPKExprInfo(
                phaseOneInfoMask, exprName, refName, commonResource);        
        ASSERT_TRUE(ret);
        ASSERT_EQ("id", exprName);
        ASSERT_EQ(RAW_PRIMARYKEY_REF, refName);
    }

    {
        TableInfoPtr tableInfoPtr = TableInfoConfigurator().createFromFile(
                TEST_DATA_PATH "/testFillAttributesOp/schema_without_primary_info.json");
        SearchCommonResourcePtr commonResource(new SearchCommonResource(nullptr, tableInfoPtr, 
                        nullptr, nullptr, nullptr, nullptr,
                        CavaPluginManagerPtr(), _request.get(), nullptr, cavaJitModules));


        uint8_t phaseOneInfoMask = 0;
        phaseOneInfoMask |= common::pob_rawpk_flag;
        std::string exprName("");
        std::string refName("");

        bool ret = _fillAttributesOp->getPKExprInfo(
                phaseOneInfoMask, exprName, refName, commonResource);
        ASSERT_FALSE(ret);
        ASSERT_EQ("", exprName);
        ASSERT_EQ("", refName);
    }

}

TEST_F(Ha3FillAttributesOpTest, testOpSimple) {
    prepareFillAttributeOp();
    initSearcherQueryResource();
    uint8_t phaseOneInfoMask = common::pob_rawpk_flag;
    uint32_t fetchSummaryType = BY_DOCID;
    vector<string> attributeNames;
    attributeNames.push_back("id");    
    attributeNames.push_back("company_id");
    attributeNames.push_back("cat_id");
    Variant variant1 = fakeHa3Request(phaseOneInfoMask, fetchSummaryType, attributeNames);


    auto matchDocAllocator = _commonResource->matchDocAllocator;
    vector<MatchDoc> matchDocVec;
    for (size_t i=0; i<6; i++) {
        matchDocVec.push_back(matchDocAllocator->allocate(i));
    }

    Variant variant2 = fakeMatchDocs(matchDocVec);
    AddInput<Variant>(
            TensorShape({}),
            [&variant1](int x) -> Variant {
                return variant1;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&variant2](int x) -> Variant {
                return variant2;
            });
    TF_ASSERT_OK(RunOpKernel());

    auto pOutputTensor = GetOutput(0);
    ASSERT_TRUE(pOutputTensor != nullptr);
    auto matchDocsVariant = pOutputTensor->scalar<Variant>()().get<MatchDocsVariant>();
    ASSERT_TRUE(matchDocsVariant != nullptr);

    matchDocVec.clear();
    matchDocsVariant->stealMatchDocs(matchDocVec);
    ASSERT_EQ(6u, matchDocVec.size());

    auto idRef = matchDocAllocator->findReference<int32_t>("id");
    ASSERT_TRUE(idRef != nullptr);
    checkAttribute(matchDocVec, idRef, "1,2,3,4,5,6");

    auto companyIdRef = matchDocAllocator->findReference<int32_t>("company_id");
    ASSERT_TRUE(companyIdRef != nullptr);
    checkAttribute(matchDocVec, companyIdRef, "1,1,1,2,2,2");

    auto catIdRef = matchDocAllocator->findReference<int32_t>("cat_id");
    ASSERT_TRUE(catIdRef != nullptr);
    checkAttribute(matchDocVec, catIdRef, "1,2,6,3,4,5");
}

END_HA3_NAMESPACE();

