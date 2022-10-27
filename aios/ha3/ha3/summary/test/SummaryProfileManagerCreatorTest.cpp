#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/ConfigAdapter.h>
#include <ha3/summary/SummaryProfileManagerCreator.h>
#include <ha3/test/test.h>
#include <ha3/summary/SummaryProfileManager.h>
#include <ha3/common/Term.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3/common/SummaryHit.h>
#include <ha3/service/SearcherResourceCreator.h>
#include <ha3/search/SearchCommonResource.h>
#include <matchdoc/MatchDocAllocator.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(summary);

class SummaryProfileManagerCreatorTest : public TESTBASE {
public:
    SummaryProfileManagerCreatorTest();
    ~SummaryProfileManagerCreatorTest();
public:
    void setUp();
    void tearDown();
protected:
    SummaryProfileManagerCreatorPtr _creatorPtr;
    config::ConfigAdapterPtr _configAdapterPtr;
    config::ResourceReaderPtr _resourceReaderPtr;
    config::HitSummarySchemaPtr _hitSummarySchemaPtr;
    autil::mem_pool::Pool *_pool;
    CavaPluginManagerPtr _cavaPluginManagerPtr;
    SearchCommonResource *_commonResource;
    Request *_request;
    suez::turing::CavaJitWrapperPtr _cavaJitWrapper;
    std::map<size_t, ::cava::CavaJitModulePtr> _cavaJitModules;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(summary, SummaryProfileManagerCreatorTest);


SummaryProfileManagerCreatorTest::SummaryProfileManagerCreatorTest() {
}

SummaryProfileManagerCreatorTest::~SummaryProfileManagerCreatorTest() {
}

void SummaryProfileManagerCreatorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    ::cava::CavaJit::globalInit();
    string configRoot = TEST_DATA_CONF_PATH"/configurations/configuration_1/";
    _configAdapterPtr.reset(new ConfigAdapter(configRoot));
    _resourceReaderPtr.reset(new ResourceReader(_configAdapterPtr->getConfigPath()));
    suez::turing::CavaConfig cavaConfigInfo;
    _configAdapterPtr->getCavaConfig("auction", cavaConfigInfo);

    _cavaPluginManagerPtr.reset(new CavaPluginManager(_cavaJitWrapper, NULL));
    std::vector<FunctionInfo> funcInfos;
    _cavaPluginManagerPtr->init(funcInfos);

    TableInfoPtr tableInfoPtr = TableInfoConfigurator::createFromFile(TEST_DATA_PATH"/config_test/sample_schema.json");
    _hitSummarySchemaPtr.reset(new HitSummarySchema(tableInfoPtr.get()));
    _creatorPtr.reset(new SummaryProfileManagerCreator(
                    _resourceReaderPtr, _hitSummarySchemaPtr,
                    _cavaPluginManagerPtr, NULL));
    _pool = new autil::mem_pool::Pool(10 * 1024 * 1024);
    _request = new Request();
    _commonResource = new SearchCommonResource(_pool, tableInfoPtr, nullptr, nullptr, nullptr,
                                  nullptr, CavaPluginManagerPtr(), _request, nullptr, _cavaJitModules);
}

void SummaryProfileManagerCreatorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    delete _commonResource;
    delete _request;
    delete _pool;
    _pool = NULL;
}

TEST_F(SummaryProfileManagerCreatorTest, testCreateFromString) {
    SummaryProfileConfig summaryConfig;
    _configAdapterPtr->getSummaryProfileConfig("auction", summaryConfig);
    SummaryProfileManagerPtr summaryPflMgrPtr = _creatorPtr->create(summaryConfig);
    ASSERT_TRUE(summaryPflMgrPtr);
    ASSERT_TRUE(summaryPflMgrPtr->init(_resourceReaderPtr, _cavaPluginManagerPtr, NULL));
}

TEST_F(SummaryProfileManagerCreatorTest, testCreateFromStringWithEmptyString) {
    string summaryConfigStr = "";
    SummaryProfileManagerPtr summaryPflMgrPtr = _creatorPtr->createFromString(summaryConfigStr);
    ASSERT_TRUE(summaryPflMgrPtr);
    ASSERT_TRUE(summaryPflMgrPtr->init(_resourceReaderPtr, _cavaPluginManagerPtr, NULL));
    const SummaryProfile* summaryProfile = summaryPflMgrPtr->getSummaryProfile("DefaultProfile");
    ASSERT_TRUE(summaryProfile);
    string queryString = "yi";
    vector<Term> terms;
    RequiredFields requiredFields;
    terms.push_back(Term("yi", "", requiredFields));
    SummaryQueryInfo queryInfo(queryString, terms);

    common::Request request;
    request.setConfigClause(new ConfigClause());
    SummaryExtractorProvider provider(&queryInfo,
            &summaryProfile->getFieldSummaryConfig(),
            &request, NULL, _hitSummarySchemaPtr.get(), *_commonResource);
    SummaryExtractorChain* summaryExtractorChain =
        summaryProfile->createSummaryExtractorChain();

    summaryExtractorChain->beginRequest(&provider);
    SummaryHit summaryHit(_hitSummarySchemaPtr.get(), _pool);
    string fieldValue = "gao\t \tyi\t \tge\t \tstring";
    int32_t summaryFieldId = 3;
    summaryHit.setFieldValue(summaryFieldId, fieldValue.data(), fieldValue.length());

    summaryExtractorChain->extractSummary(summaryHit);

    const ConstString *constStr = summaryHit.getFieldValue(summaryFieldId);
    string output(constStr->data(), constStr->size());
    ASSERT_EQ(string("gao <font color=red>yi</font> ge string"), output);
    summaryExtractorChain->destroy();
}

TEST_F(SummaryProfileManagerCreatorTest, testCreateFromStringWithSo) {
    SummaryProfileConfig summaryConfig;
    _configAdapterPtr->getSummaryProfileConfig("auction_summary_so.2.0", summaryConfig);
    SummaryProfileManagerPtr summaryPflMgrPtr = _creatorPtr->create(summaryConfig);
    ASSERT_TRUE(summaryPflMgrPtr);
    ASSERT_TRUE(summaryPflMgrPtr->init(_resourceReaderPtr, _cavaPluginManagerPtr, NULL));
    const SummaryProfile *profile = summaryPflMgrPtr->getSummaryProfile("DefaultProfile");
    string queryString = "yi";
    vector<Term> terms;
    RequiredFields requiredFields;
    terms.push_back(Term("yi", "", requiredFields));

    SummaryQueryInfo queryInfo(queryString, terms);

    common::Request request;
    request.setConfigClause(new ConfigClause());
    SummaryExtractorProvider provider(&queryInfo,
            &profile->getFieldSummaryConfig(),
            &request, NULL, _hitSummarySchemaPtr.get(), *_commonResource);
    SummaryExtractorChain* summaryExtractorChain = profile->createSummaryExtractorChain();

    summaryExtractorChain->beginRequest(&provider);
    SummaryHit summaryHit(_hitSummarySchemaPtr.get(), _pool);
    string fieldValue = "gao\t \tyi\t \tge\t \tstring";
    int32_t summaryFieldId = 3;
    summaryHit.setFieldValue(summaryFieldId, fieldValue.data(), fieldValue.length());

    summaryExtractorChain->extractSummary(summaryHit);

    const ConstString *constStr = summaryHit.getFieldValue(summaryFieldId);
    string output(constStr->data(), constStr->size());
    ASSERT_EQ(string("gao <font color=red>yi</font> ge string"), output);
    summaryExtractorChain->destroy();
}

END_HA3_NAMESPACE(summary);
