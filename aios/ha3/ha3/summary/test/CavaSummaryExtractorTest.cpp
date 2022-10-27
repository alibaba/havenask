#include <unittest/unittest.h>
#include <ha3/summary/CavaSummaryExtractor.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/test/test.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3/common/SummaryHit.h>
#include <ha3/summary/SummaryProfile.h>
#include <ha3/cava/SummaryDoc.h>
#include <ha3/service/SearcherResource.h>
#include <ha3/service/SearcherResourceCreator.h>
#include <ha3/search/IndexPartitionWrapper.h>
#include <suez/turing/common/SuezCavaAllocator.h>
#include <indexlib/partition/online_partition.h>

using namespace std;
using namespace testing;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(search);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

BEGIN_HA3_NAMESPACE(summary);

class CavaSummaryExtractorTest : public TESTBASE {
public:
    CavaSummaryExtractorTest() : _summaryExtractor(NULL) {};
    void setUp();
    void tearDown();
    bool createSearcherResource(SearcherResourcePtr &searchResourcePtr);
private:
    CavaSummaryExtractor *_summaryExtractor;
    SearcherResourcePtr _searchResourcePtr;
    SearchCommonResource *_searchCommonResource;
    Tracer *_tracer;
    Request *_request;
    autil::mem_pool::Pool *_pool;
    suez::turing::SuezCavaAllocator *_ha3CavaAllocator;
    CavaCtx *_cavaCtx;
    std::map<size_t, ::cava::CavaJitModulePtr> _cavaJitModules;
    suez::turing::CavaJitWrapperPtr _cavaJitWrapper;
    suez::turing::CavaPluginManagerPtr _cavaPluginManager;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(summary, CavaSummaryExtractorTest);

void CavaSummaryExtractorTest::setUp() {
    ::cava::CavaJit::globalInit();
    KeyValueMap para;
    para["default_class_name"] = "test.SummaryProcessor";
    suez::turing::CavaConfig cavaConfig;
    cavaConfig._enable = true;
    cavaConfig._cavaConf = "../../testdata/cava_summary/ha3_cava_config.json";
    cavaConfig._sourcePath = "cava/src";
    std::string configPath = string(TEST_DATA_CONF_PATH) + "/configurations/configuration_1/";
    _cavaJitWrapper.reset(new suez::turing::CavaJitWrapper(configPath, cavaConfig, NULL));
    ASSERT_TRUE(_cavaJitWrapper->init());
    _cavaPluginManager.reset(new CavaPluginManager(_cavaJitWrapper, NULL));
    std::vector<FunctionInfo> funcInfos;
    _cavaPluginManager->init(funcInfos);
    _summaryExtractor = new CavaSummaryExtractor(para, _cavaPluginManager, NULL);
    _tracer = new Tracer();
    _tracer->setLevel(ISEARCH_TRACE_TRACE3);
    _request = new Request();
    _pool = new autil::mem_pool::Pool(1024*1024);
    _ha3CavaAllocator = new suez::turing::SuezCavaAllocator(_pool,1024*100);
    _searchCommonResource = new SearchCommonResource(
            _pool, suez::turing::TableInfoPtr(), NULL, NULL,
            _tracer, NULL, CavaPluginManagerPtr(), _request, _ha3CavaAllocator, _cavaJitModules);

    _cavaCtx = new CavaCtx(_ha3CavaAllocator);
    //下面这段废代码是为了引入kvmapapi的符号
    KeyValueMap kv;
    ha3::KVMapApi kvmap(kv);
    kvmap.getDouble(_cavaCtx, NULL);
    kvmap.getValue(_cavaCtx, NULL);
    ha3::SummaryDoc doc(NULL);
    doc.getFieldValue(_cavaCtx, NULL);
}

void CavaSummaryExtractorTest::tearDown() {
    delete _summaryExtractor;
    delete _tracer;
    delete _request;
    delete _searchCommonResource;
    delete _pool;
    delete _ha3CavaAllocator;
    delete _cavaCtx;
}

bool CavaSummaryExtractorTest::createSearcherResource(SearcherResourcePtr &searchResourcePtr) {
    string localBaseDir = string(TEST_DATA_CONF_PATH) + "/configurations/configuration_1/";
    string configRoot = string(TEST_DATA_CONF_PATH) + "/configurations/configuration_1/";
    string dataPath1 = string(TEST_DATA_CONF_PATH) +
                      "/runtimedata/auction/auction/generation_1/partition_1/";
    string dataPath2 = string(TEST_DATA_CONF_PATH) +
                       "/runtimedata/company/company/generation_1/partition_1/";
    string clusterName = "auction.default";

    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));
    SearcherResourceCreator searcherResourceCreator(configAdapterPtr, NULL, NULL);
    IndexPartitionPtr indexPartPtr1(new OnlinePartition);
    indexPartPtr1->Open(dataPath1, "", IndexPartitionSchemaPtr(), IndexPartitionOptions());
    IndexPartitionPtr indexPartPtr2(new OnlinePartition);
    indexPartPtr2->Open(dataPath2, "", IndexPartitionSchemaPtr(), IndexPartitionOptions());
    proto::Range range;
    range.set_from(1);
    range.set_to(1);
    Cluster2IndexPartitionMapPtr cluster2IndexPartitionMapPtr(new Cluster2IndexPartitionMap);
    (*cluster2IndexPartitionMapPtr)["company"] = indexPartPtr2;

    search::IndexPartitionWrapperPtr indexPartWrapper =
        search::IndexPartitionWrapper::createIndexPartitionWrapper(configAdapterPtr,
                indexPartPtr1, clusterName);

    return searcherResourceCreator.createSearcherResource(clusterName, range,
            2, 3, indexPartWrapper, searchResourcePtr).code == ERROR_NONE;
}

TEST_F(CavaSummaryExtractorTest, testBeginRequestNoFoundClass) {
    ASSERT_TRUE(_summaryExtractor != NULL);
    Request *request = new Request();
    ConfigClause *config = new ConfigClause();
    config->addKVPair("summary_class_name","no_found");
    request->setConfigClause(config);

    SummaryQueryInfo *queryInfo = new SummaryQueryInfo();
    SummaryExtractorProvider *provider = new SummaryExtractorProvider(
            queryInfo, NULL, request, NULL,
            NULL, *_searchCommonResource);

    ASSERT_TRUE(!_summaryExtractor->beginRequest(provider));

    string traceInfo = _tracer->getTraceInfo();
    ASSERT_TRUE(traceInfo.find("no summary processor in this query")
                != string::npos);

    delete request;
    delete queryInfo;
    delete provider;
}

TEST_F(CavaSummaryExtractorTest, testFieldCopy) {

    ASSERT_TRUE(_summaryExtractor != NULL);
    Request *request = new Request();
    ConfigClause *config = new ConfigClause();
    config->addKVPair("summary_class_name","TestSummaryCopy");
    config->addKVPair("src","company_name");
    config->addKVPair("dest","company_name2");
    request->setConfigClause(config);

    SummaryQueryInfo *queryInfo = new SummaryQueryInfo();
    SummaryExtractorProvider *provider = new SummaryExtractorProvider(
            queryInfo, NULL, request, NULL,
            NULL, *_searchCommonResource);
    bool ret = _summaryExtractor->beginRequest(provider);
    string traceInfo = _tracer->getTraceInfo();
    std::cout << traceInfo << std::endl;
    ASSERT_TRUE(traceInfo.find("use summary processor [TestSummaryCopy]")
                != string::npos);
    ASSERT_TRUE(ret);

    TableInfoPtr tableInfoPtr = TableInfoConfigurator::createFromFile(
            string(TEST_DATA_CONF_PATH) + "/configurations/configuration_1/schemas/auction_schema.json");
    HitSummarySchemaPtr hitSummarySchemaPtr(new HitSummarySchema(tableInfoPtr.get()));
    common::SummaryHit hit (hitSummarySchemaPtr.get(), _pool, _tracer);
    hit.setSummaryValue("company_name", "company_name");
    hit.clearFieldValue("company_name2");
    _summaryExtractor->extractSummary(hit);
    ASSERT_TRUE(_summaryExtractor->_cavaCtx->exception == 0);
    ASSERT_TRUE(hit.getFieldValue("company_name") == NULL);
    const autil::ConstString* val = hit.getFieldValue("company_name2");
    ASSERT_TRUE(val != NULL);
    ASSERT_TRUE("company_name" == std::string(val->data(), val->size()));
    delete request;
    delete queryInfo;
    delete provider;
}

TEST_F(CavaSummaryExtractorTest, testSummaryAll) {

    ASSERT_TRUE(_summaryExtractor != NULL);
    Request *request = new Request();
    ConfigClause *config = new ConfigClause();
    config->addKVPair("summary_class_name","TestSummaryAll");
    request->setConfigClause(config);

    SummaryQueryInfo *queryInfo = new SummaryQueryInfo();
    SummaryExtractorProvider *provider = new SummaryExtractorProvider(
            queryInfo, NULL, request, NULL,
            NULL, *_searchCommonResource);
    bool ret = _summaryExtractor->beginRequest(provider);
    string traceInfo = _tracer->getTraceInfo();
    std::cout << traceInfo << std::endl;
    ASSERT_TRUE(traceInfo.find("use summary processor [TestSummaryAll]")
                != string::npos);
    ASSERT_TRUE(ret);

    TableInfoPtr tableInfoPtr = TableInfoConfigurator::createFromFile(
            string(TEST_DATA_CONF_PATH) + "/configurations/configuration_1/schemas/auction_schema.json");
    HitSummarySchemaPtr hitSummarySchemaPtr(new HitSummarySchema(tableInfoPtr.get()));
    common::SummaryHit hit (hitSummarySchemaPtr.get(), _pool, _tracer);

    //summary测试设置case
    hit.clearFieldValue("cava_copy");
    hit.clearFieldValue("cava_split");
    hit.clearFieldValue("cava_field_not_exist");

    hit.setSummaryValue("cava_copy", "cava_copy");
    hit.setSummaryValue("cava_split", "cava_split");

    _summaryExtractor->extractSummary(hit);

    //结果验证
    ASSERT_TRUE(hit.getFieldValue("cava_field_not_exist") == NULL);
    ASSERT_TRUE(hit.getFieldValue("cava_field_not_exist_2") == NULL);
    {
        std::string src = "cava_copy";
        std::string dest = "cava_copy2";
        std::string content = "cava_copy";
        const autil::ConstString* val = hit.getFieldValue(dest);
        ASSERT_TRUE(val != NULL);
        ASSERT_EQ(src,  std::string(val->data(), val->size()));
        ASSERT_TRUE(hit.getFieldValue(src) == NULL);
    }
    {
        std::string src = "cava_split";
        std::string dest = "cava_split2";
        std::string content = "split";
        const autil::ConstString* val = hit.getFieldValue(dest);
        ASSERT_TRUE(val != NULL);
        ASSERT_EQ("split", std::string(val->data(), val->size()));
        ASSERT_TRUE(hit.getFieldValue(src) == NULL);
    }

    delete request;
    delete queryInfo;
    delete provider;
}

TEST_F(CavaSummaryExtractorTest, testSummaryWithException) {

    ASSERT_TRUE(_summaryExtractor != NULL);
    Request *request = new Request();
    ConfigClause *config = new ConfigClause();
    config->addKVPair("summary_class_name","TestSummaryWithException");
    request->setConfigClause(config);

    SummaryQueryInfo *queryInfo = new SummaryQueryInfo();
    SummaryExtractorProvider *provider = new SummaryExtractorProvider(
            queryInfo, NULL, request, NULL,
            NULL, *_searchCommonResource);
    bool ret = _summaryExtractor->beginRequest(provider);
    string traceInfo = _tracer->getTraceInfo();
    std::cout << traceInfo << std::endl;
    ASSERT_TRUE(traceInfo.find("use summary processor [TestSummaryWithException]")
                != string::npos);
    ASSERT_TRUE(ret);

    TableInfoPtr tableInfoPtr = TableInfoConfigurator::createFromFile(
            string(TEST_DATA_CONF_PATH) + "/configurations/configuration_1/schemas/auction_schema.json");
    HitSummarySchemaPtr hitSummarySchemaPtr(new HitSummarySchema(tableInfoPtr.get()));
    common::SummaryHit hit (hitSummarySchemaPtr.get(), _pool, _tracer);

    //summary测试设置case
    hit.clearFieldValue("cava_copy");
    hit.clearFieldValue("cava_split");
    hit.clearFieldValue("cava_field_not_exist");

    hit.setSummaryValue("cava_copy", "cava_copy");
    hit.setSummaryValue("cava_split", "cava_split");

    _summaryExtractor->extractSummary(hit);

    //结果验证
    ASSERT_TRUE(hit.getFieldValue("cava_field_not_exist") == NULL);
    ASSERT_TRUE(hit.getFieldValue("cava_field_not_exist_2") == NULL);
    {
        std::string src = "cava_copy";
        std::string dest = "cava_copy2";
        const autil::ConstString* val = hit.getFieldValue(dest);
        ASSERT_TRUE(val == NULL);
        ASSERT_TRUE(hit.getFieldValue(src) != NULL);
    }
    {
        std::string src = "cava_split";
        std::string dest = "cava_split2";
        const autil::ConstString* val = hit.getFieldValue(dest);
        ASSERT_TRUE(val == NULL);
        ASSERT_TRUE(hit.getFieldValue(src) != NULL);
    }

    delete request;
    delete queryInfo;
    delete provider;
}


END_HA3_NAMESPACE();
