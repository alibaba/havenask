#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SummarySearcher.h>
#include <ha3/summary/SummaryProfileManager.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3/search/test/FakeSummaryExtractor.h>
#include <ha3_sdk/testlib/index/FakeSummaryReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartition.h>
#include <ha3/summary/SummaryProfileManagerCreator.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <ha3/qrs/RequestValidator.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/test/test.h>
#include <indexlib/util/hash_string.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>

using namespace std;
using namespace suez::turing;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(testlib);

USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(summary);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(func_expression);

BEGIN_HA3_NAMESPACE(search);

class SummarySearcherTest : public TESTBASE {
    //TODO: not support fetch by version
public:
    SummarySearcherTest();
    ~SummarySearcherTest();
public:
    void setUp();
    void tearDown();
protected:
    void initSummaryProfileManager();
    void makeFakeSummaryReader(FakeIndexPartitionReader *partitionReaderPtr);
    void makeFakeAttributeReader(
            FakeIndexPartitionReader *fakePartitionReader);
    IE_NAMESPACE(document)::SearchSummaryDocument *prepareSummaryDocument(
            const IE_NAMESPACE(config)::SummarySchemaPtr &summarySchemaPtr,
            const IE_NAMESPACE(config)::FieldSchemaPtr &fieldSchemaPtr);
    TableInfoPtr makeFakeTableInfo();
    common::Request* prepareRequest();
    common::RequestPtr prepareRequest(const std::string &query,
                const std::string &defaultIndexName = "phrase");
    void checkHitsInfo(Hits *);
    void checkHitsFieldValue(Hits *hits,
                             std::vector<string> fieldFetchedVec,
                             std::vector<string> fieldNotFetchedVec);
    protected:
    std::tr1::shared_ptr<autil::mem_pool::Pool> _pool;
    summary::SummaryProfileManagerPtr _summaryProfileManagerPtr;
    std::tr1::shared_ptr<SummarySearcher> _searcher;
    std::tr1::shared_ptr<SearchCommonResource> _resource;
    common::Tracer *_tracer;
    common::Request *_request;
    FakeSummaryExtractor *_fakeSummaryExtractor;
    IE_NAMESPACE(partition)::IndexPartitionReaderPtr _partitionReaderPtr;
    IndexPartitionWrapperPtr _indexPartPtr;
    IndexPartitionReaderWrapperPtr _indexPartReaderWrapperPtr;

    TableInfoPtr _tableInfoPtr;
    config::HitSummarySchemaPtr _hitSummarySchema;
    config::ResourceReaderPtr _resourceReaderPtr;
    FunctionInterfaceCreatorPtr _funcCreator;
    monitor::SessionMetricsCollectorPtr _collector;
    std::vector<std::string> _requiredAttributeFields;
    std::map<size_t, ::cava::CavaJitModulePtr> _cavaJitModules;
    map<string, uint32_t> _indexName2IdMapSwap;
    map<string, uint32_t> _attrName2IdMapSwap;
    
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, SummarySearcherTest);


const string HIGHLIGHT_BEGIN_TAG = "<font color=red>";
const string HIGHLIGHT_END_TAG = "</font>";
SummarySearcherTest::SummarySearcherTest() {
}

SummarySearcherTest::~SummarySearcherTest() {
}

void SummarySearcherTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _pool.reset(new autil::mem_pool::Pool(1024));

    _request = prepareRequest();
    _resourceReaderPtr.reset(new ResourceReader("."));

    //make IndexPartitionReader
    FakeIndexPartitionReader *fakePartitionReader = new FakeIndexPartitionReader;
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("mainTable"));
    fakePartitionReader->SetSchema(schema);
    fakePartitionReader->setIndexVersion(versionid_t(0));
    _partitionReaderPtr.reset(fakePartitionReader);

    FakeIndexPartition *fakeIndexPart = new FakeIndexPartition;
    fakeIndexPart->setIndexPartitionReader(_partitionReaderPtr);
    fakeIndexPart->setSchema(TEST_DATA_PATH, "searcher_test_schema.json");
    IndexPartitionPtr indexPart(fakeIndexPart);
    _indexPartPtr.reset(new IndexPartitionWrapper);
    _indexPartPtr->init(indexPart);
    makeFakeSummaryReader(fakePartitionReader);
    makeFakeAttributeReader(fakePartitionReader);

    //make 'TableInfo'
    _tableInfoPtr = makeFakeTableInfo();
    _hitSummarySchema.reset(new HitSummarySchema(_tableInfoPtr.get()));
    initSummaryProfileManager();
    _funcCreator.reset(new FunctionInterfaceCreator);

    _collector.reset(new SessionMetricsCollector(NULL));
    _tracer = new Tracer();
    _resource.reset(new SearchCommonResource(_pool.get(), _tableInfoPtr,
                    _collector.get(), NULL, _tracer, _funcCreator.get(), CavaPluginManagerPtr(),
                    _request, NULL, _cavaJitModules));
    _searcher.reset(new SummarySearcher(*_resource, _indexPartPtr,
                    _summaryProfileManagerPtr, _hitSummarySchema));
    _indexPartReaderWrapperPtr.reset(new IndexPartitionReaderWrapper(_indexName2IdMapSwap, _attrName2IdMapSwap, {_partitionReaderPtr}));
    _searcher->setIndexPartitionReaderWrapper(_indexPartReaderWrapperPtr);
}

void SummarySearcherTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    _fakeSummaryExtractor = NULL;
    DELETE_AND_SET_NULL(_request);
    DELETE_AND_SET_NULL(_tracer);
    _requiredAttributeFields.clear();
    _searcher.reset();
    _resource.reset();
    _summaryProfileManagerPtr.reset();
}

void SummarySearcherTest::initSummaryProfileManager() {
    SummaryProfileManagerCreator creator(_resourceReaderPtr, _hitSummarySchema);
    string configStr = FileUtil::readFile(TEST_DATA_PATH"/summary_config_for_SummarySearcherTest");
    _summaryProfileManagerPtr = creator.createFromString(configStr);
    _fakeSummaryExtractor = new FakeSummaryExtractor;
    SummaryProfileInfo summaryProfileInfo;
    summaryProfileInfo._summaryProfileName = "daogou";
    SummaryProfile *summaryProfile = new SummaryProfile(summaryProfileInfo,
            _hitSummarySchema.get());
    summaryProfile->resetSummaryExtractor(_fakeSummaryExtractor);
    _summaryProfileManagerPtr->addSummaryProfile(summaryProfile);
    _summaryProfileManagerPtr->setRequiredAttributeFields(_requiredAttributeFields);
}

void SummarySearcherTest::makeFakeAttributeReader(FakeIndexPartitionReader *fakePartitionReader)
{
    fakePartitionReader->createFakeAttributeReader<int8_t>("int8",
"1, 2, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2");
    fakePartitionReader->createFakeAttributeReader<int16_t>("int16", "1, 2, 6, 3, 4, 5");
    fakePartitionReader->createFakeAttributeReader<int32_t>("int32", "1, 2, 3, 4, 5, 6");
    fakePartitionReader->createFakeMultiAttributeReader<int32_t>("multi_int32", "1, 2, 3#1, 1, 3#2, 4, 1#1, 5, 3#1, 2, 3#3, 4, 5");
    fakePartitionReader->deleteDocuments("1,2");
    fakePartitionReader->createFakePrimaryKeyReader<uint64_t>("", "1:1,2:2;2:5");
}

void SummarySearcherTest::makeFakeSummaryReader(FakeIndexPartitionReader *fakePartitionReader) {
    const IE_NAMESPACE(config)::SummarySchemaPtr &summarySchemaPtr = _indexPartPtr->getSummarySchema();
    const IE_NAMESPACE(config)::FieldSchemaPtr &fieldSchemaPtr = _indexPartPtr->getFieldSchema();
    FakeSummaryReader *summaryReader = new FakeSummaryReader(summarySchemaPtr);
    summaryReader->AddSummaryDocument(prepareSummaryDocument(summarySchemaPtr, fieldSchemaPtr));
    summaryReader->AddSummaryDocument(prepareSummaryDocument(summarySchemaPtr, fieldSchemaPtr));
    summaryReader->AddSummaryDocument(prepareSummaryDocument(summarySchemaPtr, fieldSchemaPtr));
    fakePartitionReader->SetSummaryReader(SummaryReaderPtr(summaryReader));
}

SearchSummaryDocument* SummarySearcherTest::prepareSummaryDocument(
        const IE_NAMESPACE(config)::SummarySchemaPtr &summarySchemaPtr,
        const IE_NAMESPACE(config)::FieldSchemaPtr &fieldSchemaPtr) {
    SearchSummaryDocument *ret = new SearchSummaryDocument(NULL, 8);

    //title
    string titleValue = "玩\t开心\t厨房\t，\t有\t机会\t抽奖\t得\t淘宝\t玩偶\t（\t12\t月\t31\t日\t获奖\t名单\t公布\t）";

    //body
    string bodyValue = "各位\t亲爱\t的\t大厨\t：\t北风\t吹\t、\t寒冬\t至\t，\t“\t迎\t新年\t，\t送\t大礼\t”\t活动\t三连弹\t！\t一波\t比\t一波\t高潮\t！\t一波\t比\t一波\t惊喜\t，\t让\t你\t在\t寒冬\t感到\t暖意\t！\t第\t一波\t：\t“\t开心\t玩\t厨房\t，\t整套\t淘宝\t玩偶\t免费\t拿\t”\t：\t只要\t每天\t登入\t开心\t厨房\t，\t就会\t有\t机会\t免费\t抽奖\t拿到\t珍藏版\t全套\t淘宝\t玩偶\t！\t拿\t玩偶\t不再难\t！\t机不可失\t！\t \t每天\t登陆\t，\t就有\t机会\t！\t！\t点击\t这里\t进入\t游戏\t！";

    //description
    string descriptionValue = "开心\t厨房";

    //id
    string idValue = "开心";

    //price
    string priceValue = "900";

    //type
    string typeValue = "1";

    //userid
    string useridValue = "2";

    fieldid_t fieldId;
    summaryfieldid_t summaryFieldId;
#define SET_FIELD_VALUE(field, value)                                   \
    fieldId = fieldSchemaPtr->GetFieldId(field);                        \
    summaryFieldId = summarySchemaPtr->GetSummaryFieldId(fieldId);      \
    ret->SetFieldValue(summaryFieldId, value.data(), value.size());

    SET_FIELD_VALUE("title", titleValue);
    SET_FIELD_VALUE("body", bodyValue);
    SET_FIELD_VALUE("description", descriptionValue);
    SET_FIELD_VALUE("id", idValue);
    SET_FIELD_VALUE("price", priceValue);
    SET_FIELD_VALUE("userid", useridValue);
    SET_FIELD_VALUE("type", typeValue);
    return ret;
}

TEST_F(SummarySearcherTest, testSearchResultExceptionSummary) {
    ASSERT_TRUE(_request);
    //prepare exception partition reader

    FakeSummaryReader *summaryReader = new ExceptionSummaryReader();
    FakeIndexPartitionReaderPtr fakePartitionReaderPtr =
        std::tr1::dynamic_pointer_cast<FakeIndexPartitionReader>(_partitionReaderPtr);
    assert(fakePartitionReaderPtr);
    fakePartitionReaderPtr->SetSummaryReader(SummaryReaderPtr(summaryReader));

    ConfigClause *configClause = _request->getConfigClause();
    configClause->setPhaseNumber(SEARCH_PHASE_TWO);

    DocIdClause *docIdClause = new DocIdClause;
    docIdClause->addGlobalId(GlobalIdentifier(0, 0));

    TermVector terms;
    RequiredFields requiredFields;
    terms.push_back(isearch::common::Term("开心", "", requiredFields));
    docIdClause->setTermVector(terms);

    _request->setDocIdClause(docIdClause);
    Range range;
    ResultPtr result = _searcher->search(_request, range, FullIndexVersion(0));

    ASSERT_TRUE(result);
    ASSERT_TRUE(result->hasError());
    ErrorResults errors = result->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)1, errors.size());
    ASSERT_EQ(ERROR_SEARCH_FETCH_SUMMARY_FILEIO_ERROR, errors[0].getErrorCode());

    string errorMsg = "ExceptionBase: FileIOException";
    ASSERT_EQ(errorMsg, errors[0].getErrorMsg());

    const Hits *hits = result->getHits();
    ASSERT_TRUE(hits);
    ASSERT_EQ((uint32_t)1, hits->size());
}

TEST_F(SummarySearcherTest, testSearchResultNoSummary) {
    ASSERT_TRUE(_request);

    ConfigClause *configClause = _request->getConfigClause();
    configClause->setPhaseNumber(SEARCH_PHASE_TWO);

    DocIdClause *docIdClause = new DocIdClause;
    docIdClause->addGlobalId(GlobalIdentifier(12345, 0));

    TermVector terms;
    RequiredFields requiredFields;
    terms.push_back(isearch::common::Term("开心", "", requiredFields));
    docIdClause->setTermVector(terms);

    _request->setDocIdClause(docIdClause);
    Range range;
    ResultPtr result = _searcher->search(_request, range, FullIndexVersion(0));

    ASSERT_TRUE(result);
    ASSERT_TRUE(result->hasError());
    ErrorResults errors = result->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)1, errors.size());
    ASSERT_EQ(ERROR_SEARCH_FETCH_SUMMARY, errors[0].getErrorCode());

    const Hits *hits = result->getHits();
    ASSERT_TRUE(hits);
    ASSERT_EQ((uint32_t)1, hits->size());
}

// TODO: not support fetch by version
// TEST_F(SummarySearcherTest, testSecondPhaseSearch) {
//     HA3_LOG(DEBUG, "Begin Test!");

//     ConfigClause *configClause = _request->getConfigClause();
//     configClause->setPhaseNumber(SEARCH_PHASE_TWO);

//     DocIdClause *docIdClause = new DocIdClause;
//     docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(0), 0, 0, 0));
//     docIdClause->addGlobalId(GlobalIdentifier(-1, 2, versionid_t(0), 0, 0, 0));
//     docIdClause->addGlobalId(GlobalIdentifier(-10, 2, versionid_t(0), 0, 0, 0));
//     docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(1), 0, 0, 0));

//     TermVector terms;
//     RequiredFields requiredFields;
//     terms.push_back(isearch::common::Term("开心", "", requiredFields));
//     terms.push_back(isearch::common::Term("厨房", "", requiredFields));
//     terms.push_back(isearch::common::Term("淘宝", "", requiredFields));
//     docIdClause->setTermVector(terms);

//     _request->setDocIdClause(docIdClause);
//     Range range;
//     range.set_from(0);
//     range.set_to(65535);
//     ResultPtr result = _searcher->search(_request, range, FullIndexVersion(0));
//     ASSERT_TRUE(result);

//     ErrorResults errors = result->getMultiErrorResult()->getErrorResults();
//     ASSERT_EQ((size_t)1, errors.size());
//     ASSERT_EQ(ERROR_SEARCH_FETCH_SUMMARY, errors[0].getErrorCode());
//     ASSERT_EQ(string("no valid index partition reader."),
//                         errors[0].getErrorMsg());

//     Hits *hits = result->getHits();
//     ASSERT_EQ((uint32_t)3, hits->size());
//     HitPtr hitPtr = hits->getHit(0);
//     hitPtr->getSummaryHit()->setHitSummarySchema(_hitSummarySchema.get());
//     ASSERT_EQ((docid_t)0, hitPtr->getDocId());
//     ASSERT_EQ((uint32_t)7, hitPtr->getSummaryCount());

//     const string& titleSummary = hitPtr->getSummaryValue("title");
//     const string& bodySummary = hitPtr->getSummaryValue("body");
//     const string& descriptionSummary = hitPtr->getSummaryValue("description");
//     const string& idSummary = hitPtr->getSummaryValue("id");
//     const string& priceSummary = hitPtr->getSummaryValue("price");
//     const string& typeSummary = hitPtr->getSummaryValue("type");
//     const string& useridSummary = hitPtr->getSummaryValue("userid");

//     //title
//     string titleValue = string() + "玩" + HIGHLIGHT_BEGIN_TAG + "开心"
//                         + "厨房" + HIGHLIGHT_END_TAG
//                         + "，有机会抽奖得" + HIGHLIGHT_BEGIN_TAG + "淘宝" + HIGHLIGHT_END_TAG
//                         + "玩偶（12月31日获奖名单公布）";

//     //body
//     string bodyValue = "一波比一波高潮！一波比一波惊喜，让你在寒冬感到暖意！第一波：“"
//                        + HIGHLIGHT_BEGIN_TAG + "开心" + HIGHLIGHT_END_TAG
//                        + "玩" + HIGHLIGHT_BEGIN_TAG + "厨房" + HIGHLIGHT_END_TAG
//                        + "，整套" + HIGHLIGHT_BEGIN_TAG + "淘宝" + HIGHLIGHT_END_TAG
//                        + "玩偶免费拿”：只要每天登入"
//                        + HIGHLIGHT_BEGIN_TAG + "开心"
//                        + "厨房" + HIGHLIGHT_END_TAG + "，...";

//     //description
//     string descriptionValue = "";

//     //id
//     string idValue = "开心";

//     //price
//     string priceValue = "900";

//     //type
//     string typeValue = "1";

//     //userid
//     string useridValue = "2";

//     ASSERT_EQ(titleValue, titleSummary);
//     ASSERT_EQ(bodyValue, bodySummary);
//     ASSERT_EQ(descriptionValue, descriptionSummary);
//     ASSERT_EQ(idValue, idSummary);
//     ASSERT_EQ(priceValue, priceSummary);
//     ASSERT_EQ(typeValue, typeSummary);
//     ASSERT_EQ(useridValue, useridSummary);
//     ASSERT_EQ(hashid_t(2), hitPtr->getHashId());
//     ASSERT_EQ(FullIndexVersion(0), hitPtr->getFullIndexVersion());
//     ASSERT_EQ(versionid_t(0), hitPtr->getIndexVersion());

//     docIdClause = _request->getDocIdClause();
//     docIdClause->setSignature(_hitSummarySchema->getSignature());

//     result = _searcher->search(_request, range, FullIndexVersion(0));
//     ASSERT_TRUE(result);
// }

TableInfoPtr SummarySearcherTest::makeFakeTableInfo() {
    TableInfoConfigurator tableInfoConfigurator;
    TableInfoPtr tableInfoPtr = tableInfoConfigurator.createFromFile(
            TEST_DATA_PATH"/searcher_test_schema.json");

    assert(tableInfoPtr);

    //modify the 'Analyzer' setting of 'phrase' index. (temp solution)
    IndexInfos *indexInfos = (IndexInfos *)tableInfoPtr->getIndexInfos();
    assert(indexInfos);
    IndexInfo *indexInfo = (IndexInfo *)indexInfos->getIndexInfo("phrase");
    assert(indexInfo);
    indexInfo->setAnalyzerName("default");

    return tableInfoPtr;
}

Request *SummarySearcherTest::prepareRequest() {
    Request *request = new Request;

    QueryClause *queryClause = new QueryClause(NULL);
    request->setQueryClause(queryClause);

    ConfigClause *configClause = new ConfigClause();
    request->setConfigClause(configClause);

    return request;
}

RequestPtr SummarySearcherTest::prepareRequest(const string &query,
        const string &defaultIndexName)
{
    RequestPtr requestPtr = RequestCreator::prepareRequest(query,
            defaultIndexName);
    if (!requestPtr) {
        return requestPtr;
    }

    ClusterTableInfoMapPtr clusterTableInfoMapPtr(new ClusterTableInfoMap);
    (*clusterTableInfoMapPtr)["cluster1.default"] = _tableInfoPtr;
    ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap);
    ClusterConfigInfo clusterInfo1;
    clusterInfo1._tableName = "many";
    clusterConfigMapPtr->insert(make_pair("cluster1.default", clusterInfo1));

    RequestValidator requestValidator(clusterTableInfoMapPtr,
            10000, clusterConfigMapPtr,
            ClusterFuncMapPtr(new ClusterFuncMap), CavaPluginManagerMapPtr(),
            ClusterSorterNamesPtr(new ClusterSorterNames));
    bool ret = requestValidator.validate(requestPtr);
    if (!ret) {
        return RequestPtr();
    }
    return requestPtr;
}

TEST_F(SummarySearcherTest, testFillSummaryWithAttributeWithMuliLoops) {
    HA3_LOG(DEBUG, "Begin Test!");
    //prepare index part, two version:0,1
    FakeIndexPartitionReader *fakePartitionReader2 = new FakeIndexPartitionReader;
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("mainTable"));
    fakePartitionReader2->SetSchema(schema);
    fakePartitionReader2->setIndexVersion(versionid_t(1));
    makeFakeSummaryReader(fakePartitionReader2);
    makeFakeAttributeReader(fakePartitionReader2);
    IndexPartitionReaderPtr partitionReaderPtr2;
    partitionReaderPtr2.reset(fakePartitionReader2);
    FakeIndexPartition *fakeIndexPart =
        dynamic_cast<FakeIndexPartition *>(_indexPartPtr->getMainTablePart().get());
    ASSERT_TRUE(fakeIndexPart);
    fakeIndexPart->setIndexPartitionReader(partitionReaderPtr2);

    RequestPtr requestPtr = prepareRequest(
                "config=cluster:cluster1&&query=phrase:with"
                "&&virtual_attribute=va1:int16 + 4");
    ASSERT_TRUE(requestPtr);

    DocIdClause *docIdClause = new DocIdClause;
    docIdClause->setSummaryProfileName("daogou");
    docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(0), 0, 0, 0));
    docIdClause->addGlobalId(GlobalIdentifier(1, 2, versionid_t(0), 0, 0, 0));
    docIdClause->addGlobalId(GlobalIdentifier(2, 2, versionid_t(0), 0, 0, 0));
    requestPtr->setDocIdClause(docIdClause);

    vector<string> attributes;
    attributes.push_back("int8");
    attributes.push_back("va1");
    _fakeSummaryExtractor->setFilledAttributes(attributes);

    Range range;
    range.set_from(0);
    range.set_to(65535);
    IndexPartitionReaderWrapperPtr  indexPartReaderWrapper2(new IndexPartitionReaderWrapper(_indexName2IdMapSwap, _attrName2IdMapSwap, {partitionReaderPtr2}));
    _searcher->_attributeExpressionCreator = new FakeAttributeExpressionCreator(
            _pool.get(),
            indexPartReaderWrapper2,
            NULL, NULL, requestPtr->getVirtualAttributeClause(), NULL, NULL);
    ResultPtr result = _searcher->search(requestPtr.get(), range, FullIndexVersion(0));
    delete _searcher->_attributeExpressionCreator;
    _searcher->_attributeExpressionCreator = NULL;
    ASSERT_TRUE(result);
    ASSERT_TRUE(!result->hasError());

    Hits *hits = result->getHits();
    ASSERT_EQ((uint32_t)3, hits->size());
    HitPtr hitPtr;
    for (int i = 0; i < 3; ++i)
    {
        hitPtr = hits->getHit(i);
        HitSummarySchemaMap hitSummarySchemaMap = hits->getHitSummarySchemaMap();
        ASSERT_EQ((size_t)1, hitSummarySchemaMap.size());
        HitSummarySchemaMap::iterator it = hitSummarySchemaMap.begin();
        ASSERT_EQ(hitPtr->getSummaryHit()->getSignature(), it->first);
        hitPtr->getSummaryHit()->setHitSummarySchema(it->second);
        ASSERT_EQ((docid_t)i, hitPtr->getDocId());
        ASSERT_EQ((uint32_t)9, hitPtr->getSummaryCount());
    }

    hitPtr = hits->getHit(0);
    const string& int8Str = hitPtr->getSummaryValue("int8");
    const string& va1Str = hitPtr->getSummaryValue("va1");

    ASSERT_EQ(string("1"), int8Str);
    ASSERT_EQ(string("5"), va1Str);

    hitPtr = hits->getHit(1);
    const string& int8Str2 = hitPtr->getSummaryValue("int8");
    const string& va1Str2 = hitPtr->getSummaryValue("va1");

    ASSERT_EQ(string("2"), int8Str2);
    ASSERT_EQ(string("6"), va1Str2);


    hitPtr = hits->getHit(2);
    const string& int8Str3 = hitPtr->getSummaryValue("int8");
    const string& va1Str3 = hitPtr->getSummaryValue("va1");

    ASSERT_EQ(string("1"), int8Str3);
    ASSERT_EQ(string("10"), va1Str3);

}

TEST_F(SummarySearcherTest, testSummaryExtractorBeginRequestFailed) {
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr requestPtr = prepareRequest(
                "config=cluster:cluster1&&query=phrase:with"
                "&&virtual_attribute=va1:int16 + 4");
    ASSERT_TRUE(requestPtr);
    DocIdClause *docIdClause = new DocIdClause;
    docIdClause->setSummaryProfileName("daogou");
    docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(0), 0, 0, 0));
    requestPtr->setDocIdClause(docIdClause);

    Range range;
    range.set_from(0);
    range.set_to(65535);
    _fakeSummaryExtractor->setBeginRequestSuccessFlag(false);
    ResultPtr result = _searcher->search(requestPtr.get(), range, FullIndexVersion(0));

    ErrorResults errors = result->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)1, errors.size());
    ASSERT_EQ(ERROR_SUMMARY_EXTRACTOR_BEGIN_REQUEST,
                         errors[0].getErrorCode());

}

TEST_F(SummarySearcherTest, testSummaryProfileNotExist) {
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr requestPtr = prepareRequest(
                "config=cluster:cluster1&&query=phrase:with"
                "&&virtual_attribute=va1:int16 + 4");
    ASSERT_TRUE(requestPtr);
    DocIdClause *docIdClause = new DocIdClause;
    docIdClause->setSummaryProfileName("notExistProfileName");
    docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(0), 0, 0, 0));
    requestPtr->setDocIdClause(docIdClause);

    Range range;
    range.set_from(0);
    range.set_to(65535);
    ResultPtr result = _searcher->search(requestPtr.get(), range, FullIndexVersion(0));

    ErrorResults errors = result->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ((size_t)1, errors.size());
    ASSERT_EQ(ERROR_NO_SUMMARYPROFILE, errors[0].getErrorCode());
    ASSERT_EQ(string("fail to get summary profile profile name:"
                                "notExistProfileName"),errors[0].getErrorMsg());

}

TEST_F(SummarySearcherTest, testFillSummaryWithAttribute) {
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr requestPtr = prepareRequest(
                "config=cluster:cluster1&&query=phrase:with"
                "&&virtual_attribute=va1:int16 + 4");
    ASSERT_TRUE(requestPtr);

    DocIdClause *docIdClause = new DocIdClause;
    docIdClause->setSummaryProfileName("daogou");
    docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(0), 0, 0, 0));
    requestPtr->setDocIdClause(docIdClause);

    vector<string> attributes;
    attributes.push_back("int8");
    attributes.push_back("va1");
    attributes.push_back("multi_int32");
    _fakeSummaryExtractor->setFilledAttributes(attributes);

    Range range;
    range.set_from(0);
    range.set_to(65535);
    _searcher->_attributeExpressionCreator = new FakeAttributeExpressionCreator(
            _pool.get(),
            _indexPartReaderWrapperPtr,
            NULL, NULL, requestPtr->getVirtualAttributeClause(), NULL, NULL);
    ResultPtr result = _searcher->search(requestPtr.get(), range, FullIndexVersion(0));
    delete _searcher->_attributeExpressionCreator;
    _searcher->_attributeExpressionCreator = NULL;
    ASSERT_TRUE(result);
    ASSERT_TRUE(!result->hasError());

    Hits *hits = result->getHits();
    ASSERT_EQ((uint32_t)1, hits->size());
    HitPtr hitPtr = hits->getHit(0);
    HitSummarySchemaMap hitSummarySchemaMap = hits->getHitSummarySchemaMap();
    ASSERT_EQ((size_t)1, hitSummarySchemaMap.size());
    HitSummarySchemaMap::iterator it = hitSummarySchemaMap.begin();
    ASSERT_EQ(hitPtr->getSummaryHit()->getSignature(), it->first);
    hitPtr->getSummaryHit()->setHitSummarySchema(it->second);
    ASSERT_EQ((docid_t)0, hitPtr->getDocId());
    ASSERT_EQ((uint32_t)10, hitPtr->getSummaryCount());

    const string& int8Str = hitPtr->getSummaryValue("int8");
    const string& va1Str = hitPtr->getSummaryValue("va1");
    const string& multiStr = hitPtr->getSummaryValue("multi_int32");
    ASSERT_EQ(string("1"), int8Str);
    ASSERT_EQ(string("5"), va1Str);
    ASSERT_EQ(string("123"), multiStr);
}

TEST_F(SummarySearcherTest, testRequireAttributeInSummaryProfile) {
    HA3_LOG(DEBUG, "Begin Test!");

    tearDown();
    _requiredAttributeFields.push_back("int8");
    _requiredAttributeFields.push_back("multi_int32");
    setUp();

    RequestPtr requestPtr = prepareRequest(
                "config=cluster:cluster1&&query=phrase:with");
    ASSERT_TRUE(requestPtr);

    DocIdClause *docIdClause = new DocIdClause;
    docIdClause->setSummaryProfileName("daogou");
    docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(0), 0, 0, 0));
    requestPtr->setDocIdClause(docIdClause);

    Range range;
    range.set_from(0);
    range.set_to(65535);
    _searcher->_attributeExpressionCreator = new FakeAttributeExpressionCreator(
            _pool.get(),
            _indexPartReaderWrapperPtr,
            NULL, NULL, requestPtr->getVirtualAttributeClause(), NULL, NULL);
    ResultPtr result = _searcher->search(requestPtr.get(), range, FullIndexVersion(0));
    delete _searcher->_attributeExpressionCreator;
    _searcher->_attributeExpressionCreator = NULL;

    ASSERT_TRUE(result);
    ASSERT_TRUE(!result->hasError());

    Hits *hits = result->getHits();
    ASSERT_EQ((uint32_t)1, hits->size());
    HitPtr hitPtr = hits->getHit(0);
    HitSummarySchemaMap hitSummarySchemaMap = hits->getHitSummarySchemaMap();
    ASSERT_EQ((size_t)1, hitSummarySchemaMap.size());
    HitSummarySchemaMap::iterator it = hitSummarySchemaMap.begin();
    ASSERT_EQ(hitPtr->getSummaryHit()->getSignature(), it->first);
    hitPtr->getSummaryHit()->setHitSummarySchema(it->second);
    ASSERT_EQ((docid_t)0, hitPtr->getDocId());
    ASSERT_EQ((uint32_t)9, hitPtr->getSummaryCount());

    const string& int8Str = hitPtr->getSummaryValue("int8");
    const string& multiStr = hitPtr->getSummaryValue("multi_int32");
    ASSERT_EQ(string("1"), int8Str);
    ASSERT_EQ(string("123"), multiStr);
}

TEST_F(SummarySearcherTest, testFillSummaryWithAttributeNotSameSummaryCluster) {
    // summary have attr in virtual attr
    RequestPtr requestPtr = prepareRequest(
            "config=cluster:cluster1,fetch_summary_cluster:cluster2"
            "&&query=phrase:with&&virtual_attribute=va1:int16 + 4");
    ASSERT_TRUE(requestPtr);

    DocIdClause *docIdClause = new DocIdClause;
    docIdClause->setSummaryProfileName("daogou");
    docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(0), 0, 0, 0));
    requestPtr->setDocIdClause(docIdClause);

    vector<string> attributes;
    attributes.push_back("va1");
    _fakeSummaryExtractor->setFilledAttributes(attributes);

    Range range;
    range.set_from(0);
    range.set_to(65535);
    _searcher->_attributeExpressionCreator = new FakeAttributeExpressionCreator(
            _pool.get(),
            _indexPartReaderWrapperPtr,
            NULL, NULL, requestPtr->getVirtualAttributeClause(), NULL, NULL);
    ResultPtr result = _searcher->search(requestPtr.get(), range, FullIndexVersion(0));
    delete _searcher->_attributeExpressionCreator;
    _searcher->_attributeExpressionCreator = NULL;
    ASSERT_TRUE(result);
    ASSERT_TRUE(!result->hasError());
}

TEST_F(SummarySearcherTest, testFetchSummaryTimeout) {
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr requestPtr = prepareRequest(
                "config=cluster:cluster1&&query=phrase:with");
    ASSERT_TRUE(requestPtr);

    DocIdClause *docIdClause = new DocIdClause;
    docIdClause->setSummaryProfileName("daogou");
    docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(0), 0, 0, 0));
    docIdClause->addGlobalId(GlobalIdentifier(1, 2, versionid_t(0), 0, 0, 0));
    requestPtr->setDocIdClause(docIdClause);

    Range range;
    range.set_from(0);
    range.set_to(65535);
    ResultPtr result = _searcher->search(requestPtr.get(), range, FullIndexVersion(0));
    ASSERT_TRUE(result);
    ASSERT_TRUE(!result->hasError());

    Hits *hits = result->getHits();
    ASSERT_EQ((uint32_t)2, hits->size());
    ASSERT_TRUE (hits->getHit(0)->getSummaryHit());
    ASSERT_TRUE (hits->getHit(1)->getSummaryHit());

    TimeoutTerminator timeoutTerminator(1, 1);
    _searcher->_resource.timeoutTerminator = &timeoutTerminator;

    result = _searcher->search(requestPtr.get(), range, FullIndexVersion(0));
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->hasError());
    const ErrorResults& errorResults =
        result->getMultiErrorResult()->getErrorResults();
    ASSERT_EQ(size_t(1), errorResults.size());
    ASSERT_EQ(ERROR_FETCH_SUMMARY_TIMEOUT,
                         errorResults[0].getErrorCode());

    hits = result->getHits();
    ASSERT_EQ((uint32_t)2, hits->size());
    ASSERT_TRUE (!hits->getHit(0)->getSummaryHit());
    ASSERT_TRUE (!hits->getHit(1)->getSummaryHit());
}

TEST_F(SummarySearcherTest, testFetchSummaryWithDeletedWithPK) {
    RequestPtr requestPtr = prepareRequest(
            "config=cluster:cluster1,fetch_summary_type:pk&&query=phrase:with");
    ASSERT_TRUE(requestPtr);
    GlobalIdVector gids;
    IE_NAMESPACE(util)::HashString hashFunc;
    uint64_t hashKey;
    hashFunc.Hash(hashKey, "1", 1);
    gids.push_back(GlobalIdentifier(0, 2, versionid_t(0), 0, hashKey, 0));
    hashFunc.Hash(hashKey, "2", 1);
    gids.push_back(GlobalIdentifier(1, 2, versionid_t(0), 0, hashKey, 0));
    hashFunc.Hash(hashKey, "3", 1);
    gids.push_back(GlobalIdentifier(2, 2, versionid_t(0), 0, hashKey, 0));
    _searcher->createIndexPartReaderWrapper(INVALID_VERSION);
    {
        GlobalIdVector testGids = gids;
        ASSERT_TRUE(_searcher->convertPK2DocId(testGids,
                        _searcher->_indexPartitionReaderWrapper, true));

        ASSERT_EQ(docid_t(1), testGids[0].getDocId());
        ASSERT_EQ(docid_t(7), testGids[1].getDocId());
        ASSERT_EQ(docid_t(INVALID_DOCID), testGids[2].getDocId());
    }
    {
        GlobalIdVector testGids = gids;
        ASSERT_TRUE(_searcher->convertPK2DocId(testGids,
                        _searcher->_indexPartitionReaderWrapper, false));

        ASSERT_EQ(docid_t(INVALID_DOCID), testGids[0].getDocId());
        ASSERT_EQ(docid_t(7), testGids[1].getDocId());
        ASSERT_EQ(docid_t(INVALID_DOCID), testGids[2].getDocId());
    }
}

TEST_F(SummarySearcherTest, testCreateTracer) {
    RequestPtr requestPtr = prepareRequest(
            "config=trace:trace3,cluster:cluster1&&query=phrase:with");
    ASSERT_TRUE(requestPtr);

    DocIdClause *docIdClause = new DocIdClause;
    docIdClause->setSummaryProfileName("daogou");
    docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(0), 0, 0, 0));
    docIdClause->addGlobalId(GlobalIdentifier(1, 2, versionid_t(0), 0, 0, 0));
    requestPtr->setDocIdClause(docIdClause);

    Range range;
    range.set_from(0);
    range.set_to(65535);
    _resource->tracer->trace("query trace test");
    ResultPtr result = _searcher->search(requestPtr.get(), range, FullIndexVersion(0));
    ASSERT_TRUE(result);
    ASSERT_TRUE(!result->hasError());
    ASSERT_EQ(_searcher->_traceLevel, ISEARCH_TRACE_TRACE3);
    ASSERT_EQ(_searcher->_traceMode, Tracer::TM_VECTOR);

    Hits *hits = result->getHits();
    ASSERT_EQ((uint32_t)2, hits->size());
    ASSERT_TRUE (hits->getHit(0)->getSummaryHit());
    ASSERT_TRUE (hits->getHit(1)->getSummaryHit());
    ASSERT_TRUE(hits->getHit(0)->getTracer() != NULL);
    ASSERT_TRUE(hits->getHit(1)->getTracer() != NULL);
    ASSERT_TRUE(hits->getHit(1)->getSummaryHit()->getHitTracer() != NULL);
    ASSERT_TRUE(hits->getHit(0)->getSummaryHit()->getHitTracer() != NULL);
    hits->getHit(0)->getTracer()->trace("hit 0 tracer info");
    hits->getHit(1)->getTracer()->trace("hit 1 tracer info");
    ASSERT_EQ("hit 0 tracer info\n", hits->getHit(0)->getTracer()->getTraceInfo());
    ASSERT_EQ("hit 1 tracer info\n", hits->getHit(1)->getTracer()->getTraceInfo());
}

TEST_F(SummarySearcherTest, testCreateTracerByPk) {
    RequestPtr requestPtr = prepareRequest(
            "config=trace:trace3,cluster:cluster1,fetch_summary_type:pk&&query=phrase:with");
    ASSERT_TRUE(requestPtr);
    DocIdClause *docIdClause = new DocIdClause;
    IE_NAMESPACE(util)::HashString hashFunc;
    uint64_t hashKey;
    docIdClause->setSummaryProfileName("daogou");
    hashFunc.Hash(hashKey, "1", 1);
    docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(0), 0, hashKey, 0));
    // hashFunc.Hash(hashKey, "2", 1);
    // docIdClause->addGlobalId(GlobalIdentifier(1, 2, versionid_t(0), 0, hashKey, 0));
    requestPtr->setDocIdClause(docIdClause);

    Range range;
    range.set_from(0);
    range.set_to(65535);
    ResultPtr result = _searcher->search(requestPtr.get(), range, FullIndexVersion(0));
    ASSERT_TRUE(result);
    ASSERT_TRUE(!result->hasError());
    ASSERT_EQ(_searcher->_traceLevel, ISEARCH_TRACE_TRACE3);
    ASSERT_EQ(_searcher->_traceMode, Tracer::TM_VECTOR);

    Hits *hits = result->getHits();
    ASSERT_EQ((uint32_t)1, hits->size());
    ASSERT_TRUE (hits->getHit(0)->getSummaryHit());
//    ASSERT_TRUE (hits->getHit(1)->getSummaryHit());
    ASSERT_TRUE(hits->getHit(0)->getTracer() != NULL);
//    ASSERT_TRUE(hits->getHit(1)->getTracer() != NULL);
    ASSERT_TRUE(hits->getHit(0)->getSummaryHit()->getHitTracer() != NULL);
//    ASSERT_TRUE(hits->getHit(1)->getSummaryHit()->getHitTracer() != NULL);

}

TEST_F(SummarySearcherTest, testGenSummaryGroupIdVec) {
    {
        RequestPtr requestPtr = prepareRequest(
                "config=cluster:cluster1,fetch_summary_group:tpp | mainse&&query=phrase:with");
        ConfigClause *configClause = requestPtr->getConfigClause();
        _searcher->genSummaryGroupIdVec(configClause);
        SummaryGroupIdVec expectedSummaryGroupIdVec;
        expectedSummaryGroupIdVec.push_back(1);
        expectedSummaryGroupIdVec.push_back(2);
        ASSERT_EQ(expectedSummaryGroupIdVec, _searcher->getSummaryGroupIdVec());
    }
    {
        RequestPtr requestPtr = prepareRequest(
                "config=cluster:cluster1,fetch_summary_group:inshop|mainse&&query=phrase:with");
        ConfigClause *configClause = requestPtr->getConfigClause();
        _searcher->genSummaryGroupIdVec(configClause);
        ASSERT_TRUE(_searcher->getSummaryGroupIdVec().empty());
    }
 }

void SummarySearcherTest::checkHitsInfo(Hits *hits) {
    ASSERT_EQ((uint32_t)3, hits->size());
    HitPtr hitPtr;
    HitSummarySchemaMap hitSummarySchemaMap = hits->getHitSummarySchemaMap();
    ASSERT_EQ((size_t)1, hitSummarySchemaMap.size());
    for (int i = 0; i < 3; ++i)
    {
        hitPtr = hits->getHit(i);
        HitSummarySchemaMap::iterator it = hitSummarySchemaMap.begin();
        ASSERT_EQ(hitPtr->getSummaryHit()->getSignature(), it->first);
        hitPtr->getSummaryHit()->setHitSummarySchema(it->second);
        ASSERT_EQ((docid_t)i, hitPtr->getDocId());
        ASSERT_EQ((uint32_t)7, hitPtr->getSummaryCount());
    }
}

void SummarySearcherTest::checkHitsFieldValue(Hits *hits,
        std::vector<string> fieldFetchedVec,
        std::vector<string> fieldNotFetchedVec)
{
    std::map<string, string> expectedFieldValueMap;
    expectedFieldValueMap["title"] = "玩\t开心\t厨房\t，\t有\t机会\t抽奖\t得\t淘宝\t玩偶\t（\t12\t月\t31\t日\t获奖\t名单\t公布\t）";
    expectedFieldValueMap["body"] = "各位\t亲爱\t的\t大厨\t：\t北风\t吹\t、\t寒冬\t至\t，\t“\t迎\t新年\t，\t送\t大礼\t”\t活动\t三连弹\t！\t一波\t比\t一波\t高潮\t！\t一波\t比\t一波\t惊喜\t，\t让\t你\t在\t寒冬\t感到\t暖意\t！\t第\t一波\t：\t“\t开心\t玩\t厨房\t，\t整套\t淘宝\t玩偶\t免费\t拿\t”\t：\t只要\t每天\t登入\t开心\t厨房\t，\t就会\t有\t机会\t免费\t抽奖\t拿到\t珍藏版\t全套\t淘宝\t玩偶\t！\t拿\t玩偶\t不再难\t！\t机不可失\t！\t \t每天\t登陆\t，\t就有\t机会\t！\t！\t点击\t这里\t进入\t游戏\t！";
    expectedFieldValueMap["description"] = "开心\t厨房";
    expectedFieldValueMap["id"] = "开心";
    expectedFieldValueMap["userid"] = "2";
    expectedFieldValueMap["type"] = "1";
    expectedFieldValueMap["price"] = "900";

    HitPtr hitPtr;
    for (size_t i=0; i<hits->size(); i++) {
        hitPtr = hits->getHit(i);
        for (const auto &field : fieldFetchedVec) {
            const string& fieldValue = hitPtr->getSummaryValue(field);
            ASSERT_EQ(expectedFieldValueMap[field], fieldValue);
        }
        for (const auto &field : fieldNotFetchedVec) {
            const string& fieldValue = hitPtr->getSummaryValue(field);
            ASSERT_EQ("", fieldValue);
        }
    }
}

TEST_F(SummarySearcherTest, testFetchSummaryGroup) {
    HA3_LOG(DEBUG, "Begin Test!");
    //prepare index part, two version:0,1
    FakeIndexPartitionReader *fakePartitionReader2 = new FakeIndexPartitionReader;
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("mainTable"));
    fakePartitionReader2->SetSchema(schema);
    fakePartitionReader2->setIndexVersion(versionid_t(1));
    makeFakeSummaryReader(fakePartitionReader2);
    IndexPartitionReaderPtr partitionReaderPtr2;
    partitionReaderPtr2.reset(fakePartitionReader2);
    FakeIndexPartition *fakeIndexPart =
        dynamic_cast<FakeIndexPartition *>(_indexPartPtr->getMainTablePart().get());
    ASSERT_TRUE(fakeIndexPart);
    fakeIndexPart->setIndexPartitionReader(partitionReaderPtr2);


    Range range;
    range.set_from(0);
    range.set_to(65535);

    {
        DocIdClause *docIdClause = new DocIdClause;
        docIdClause->setSummaryProfileName("daogou");
        docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(0), 0, 0, 0));
        docIdClause->addGlobalId(GlobalIdentifier(1, 2, versionid_t(0), 0, 0, 0));
        docIdClause->addGlobalId(GlobalIdentifier(2, 2, versionid_t(1), 0, 0, 0));

        // normal group
        RequestPtr requestPtr = prepareRequest(
                "config=cluster:cluster1,fetch_summary_group:tpp|mainse"
                "&&query=phrase:with");
        ASSERT_TRUE(requestPtr);
        requestPtr->setDocIdClause(docIdClause);

        ResultPtr result = _searcher->search(requestPtr.get(), range, FullIndexVersion(0));
        ASSERT_TRUE(result);
        ASSERT_TRUE(!result->hasError());

        Hits *hits = result->getHits();
        checkHitsInfo(hits);
        std::vector<string> fieldFetchedVec;
        std::vector<string> fieldNotFetchedVec;
        fieldFetchedVec.push_back("title");
        fieldFetchedVec.push_back("body");
        fieldFetchedVec.push_back("description");
        fieldNotFetchedVec.push_back("price");
        fieldNotFetchedVec.push_back("userid");
        fieldNotFetchedVec.push_back("type");
        fieldNotFetchedVec.push_back("id");
        checkHitsFieldValue(hits, fieldFetchedVec, fieldNotFetchedVec);
    }
    {
        DocIdClause *docIdClause = new DocIdClause;
        docIdClause->setSummaryProfileName("daogou");
        docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(0), 0, 0, 0));
        docIdClause->addGlobalId(GlobalIdentifier(1, 2, versionid_t(0), 0, 0, 0));
        docIdClause->addGlobalId(GlobalIdentifier(2, 2, versionid_t(1), 0, 0, 0));
        // wrong group fetch all feild
        RequestPtr requestPtr = prepareRequest(
                "config=cluster:cluster1,fetch_summary_group:test|mainse"
                "&&query=phrase:with");
        ASSERT_TRUE(requestPtr);
        requestPtr->setDocIdClause(docIdClause);

        ResultPtr result = _searcher->search(requestPtr.get(), range, FullIndexVersion(0));
        ASSERT_TRUE(result);
        ASSERT_TRUE(!result->hasError());

        Hits *hits = result->getHits();
        checkHitsInfo(hits);

        std::vector<string> fieldFetchedVec;
        std::vector<string> fieldNotFetchedVec;
        fieldFetchedVec.push_back("title");
        fieldFetchedVec.push_back("body");
        fieldFetchedVec.push_back("description");
        fieldFetchedVec.push_back("price");
        fieldFetchedVec.push_back("userid");
        fieldFetchedVec.push_back("type");
        fieldFetchedVec.push_back("id");
        checkHitsFieldValue(hits, fieldFetchedVec, fieldNotFetchedVec);
    }
    {
        DocIdClause *docIdClause = new DocIdClause;
        docIdClause->setSummaryProfileName("daogou");
        docIdClause->addGlobalId(GlobalIdentifier(0, 2, versionid_t(0), 0, 0, 0));
        docIdClause->addGlobalId(GlobalIdentifier(1, 2, versionid_t(0), 0, 0, 0));
        docIdClause->addGlobalId(GlobalIdentifier(2, 2, versionid_t(1), 0, 0, 0));
        // default fetch all feilds
        RequestPtr requestPtr = prepareRequest(
                "config=cluster:cluster1"
                "&&query=phrase:with");
        ASSERT_TRUE(requestPtr);
        requestPtr->setDocIdClause(docIdClause);

        ResultPtr result = _searcher->search(requestPtr.get(), range, FullIndexVersion(0));
        ASSERT_TRUE(result);
        ASSERT_TRUE(!result->hasError());

        Hits *hits = result->getHits();
        checkHitsInfo(hits);
        std::vector<string> fieldFetchedVec;
        std::vector<string> fieldNotFetchedVec;
        fieldFetchedVec.push_back("title");
        fieldFetchedVec.push_back("body");
        fieldFetchedVec.push_back("description");
        fieldFetchedVec.push_back("price");
        fieldFetchedVec.push_back("userid");
        fieldFetchedVec.push_back("type");
        fieldFetchedVec.push_back("id");
        checkHitsFieldValue(hits, fieldFetchedVec, fieldNotFetchedVec);
        }
}

END_HA3_NAMESPACE(search);
