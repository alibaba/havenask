#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/SummaryProfileInfo.h>
#include <ha3/config/ResourceReader.h>
#include <ha3/test/test.h>
#include <ha3/summary/SummaryProfile.h>
#include <build_service/plugin/PlugInManager.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3/config/ConfigAdapter.h>
#include <suez/turing/common/CavaJitWrapper.h>
using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(summary);

class SummaryProfileTest : public TESTBASE {
public:
    SummaryProfileTest();
    ~SummaryProfileTest();
public:
    void setUp();
    void tearDown();
protected:
    config::ResourceReaderPtr _resourceReaderPtr;
    config::HitSummarySchemaPtr _hitSummarySchemaPtr;
    config::ConfigAdapterPtr _configAdapterPtr;
    CavaPluginManagerPtr _cavaPluginManagerPtr;
    CavaJitWrapperPtr _cavaJitWrapper;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(summary, SummaryProfileTest);

using namespace build_service::plugin;

SummaryProfileTest::SummaryProfileTest() {
}

SummaryProfileTest::~SummaryProfileTest() {
}

void SummaryProfileTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    ::cava::CavaJit::globalInit();

    auto tableInfoPtr = TableInfoConfigurator::createFromFile(TEST_DATA_PATH"/config_test/sample_schema.json");
    _hitSummarySchemaPtr.reset(new HitSummarySchema(tableInfoPtr.get()));

    string configRoot = TEST_DATA_CONF_PATH"/configurations/configuration_1/";
    _configAdapterPtr.reset(new ConfigAdapter(configRoot));
    _resourceReaderPtr.reset(new ResourceReader(_configAdapterPtr->getConfigPath()));
    suez::turing::CavaConfig cavaConfigInfo;
    _configAdapterPtr->getCavaConfig("auction", cavaConfigInfo);
    _cavaPluginManagerPtr.reset(new CavaPluginManager(_cavaJitWrapper, NULL));
//    _cavaPluginManagerPtr->init();
}

void SummaryProfileTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SummaryProfileTest, testConstruct) {
    HA3_LOG(DEBUG, "Begin Test!");
    string configStr =  "{\
        \"summary_profile_name\": \"DefaultProfile\",\
        \"extractor\" :{\
            \"extractor_name\" : \"extractor_1\",\
            \"module_name\" : \"fake_extractor\",\
            \"parameters\" : {\
                \"key\": \"value\"\
            }\
        },\
        \"field_configs\" : {\
            \"id\" : {\
                \"max_snippet\" : 1,\
                \"max_summary_length\" : 150,\
                \"abstract\" : true,\
                \"highlight_prefix\": \"<tag>\",\
                \"highlight_suffix\": \"</tag>\",\
                \"snippet_delimiter\": \"...\"\
            }\
        }\
    }";
    SummaryProfileInfo summaryProfileInfo;
    FromJsonString(summaryProfileInfo, configStr);
    SummaryProfile summaryProfile(summaryProfileInfo, _hitSummarySchemaPtr.get());
}

TEST_F(SummaryProfileTest, testInitBuildInModuleFailed) {
    HA3_LOG(DEBUG, "Begin Test!");
    string configStr =  "{\
        \"summary_profile_name\": \"DefaultProfile\",\
        \"extractor\" :{\
            \"extractor_name\" : \"extractor_1\",\
            \"module_name\" : \"\",\
            \"parameters\" : {\
                \"key\": \"value\"\
            }\
        },\
        \"field_configs\" : {\
            \"id\" : {\
                \"max_snippet\" : 1,\
                \"max_summary_length\" : 150,\
                \"abstract\" : true,\
                \"highlight_prefix\": \"<tag>\",\
                \"highlight_suffix\": \"</tag>\",\
                \"snippet_delimiter\": \"...\"\
            }\
        }\
    }";
    SummaryProfileInfo summaryProfileInfo;
    FromJsonString(summaryProfileInfo, configStr);
    SummaryProfile summaryProfile(summaryProfileInfo, _hitSummarySchemaPtr.get());
    PlugInManagerPtr plugInManagerPtr;
    bool ret = summaryProfile.init(plugInManagerPtr, _resourceReaderPtr, _cavaPluginManagerPtr, NULL);
    ASSERT_TRUE(!ret);

    string configStr2 =  "{\
        \"summary_profile_name\": \"DefaultProfile\",\
        \"extractor\" :{\
            \"extractor_name\" : \"extractor_1\",\
            \"module_name\" : \"BuildInModule\",\
            \"parameters\" : {\
                \"key\": \"value\"\
            }\
        },\
        \"field_configs\" : {\
            \"keywords\" : {\
                \"max_snippet\" : 1,\
                \"max_summary_length\" : 150,\
                \"abstract\" : true,\
                \"highlight_prefix\": \"<tag>\",\
                \"highlight_suffix\": \"</tag>\",\
                \"snippet_delimiter\": \"...\"\
            }\
        }\
    }";

    FromJsonString(summaryProfileInfo, configStr2);
    SummaryProfile summaryProfile2(summaryProfileInfo, _hitSummarySchemaPtr.get());
    ret = summaryProfile2.init(plugInManagerPtr, _resourceReaderPtr, _cavaPluginManagerPtr, NULL);
    ASSERT_TRUE(!ret);
}

TEST_F(SummaryProfileTest, testInitBuildInModuleSuccess) {
    HA3_LOG(DEBUG, "Begin Test!");
    string configStr =  "{\
        \"summary_profile_name\": \"DefaultProfile\",\
        \"extractor\" :{\
            \"extractor_name\" : \"DefaultSummaryExtractor\",\
            \"module_name\" : \"\",\
            \"parameters\" : {\
                \"key\": \"value\"\
            }\
        },\
        \"field_configs\" : {\
            \"keywords\" : {\
                \"max_snippet\" : 1,\
                \"max_summary_length\" : 150,\
                \"abstract\" : true,\
                \"highlight_prefix\": \"<tag>\",\
                \"highlight_suffix\": \"</tag>\",\
                \"snippet_delimiter\": \"...\"\
            }\
        }\
    }";
    SummaryProfileInfo summaryProfileInfo;
    FromJsonString(summaryProfileInfo, configStr);
    SummaryProfile summaryProfile(summaryProfileInfo, _hitSummarySchemaPtr.get());
    PlugInManagerPtr plugInManagerPtr;
    bool ret = summaryProfile.init(plugInManagerPtr, _resourceReaderPtr, _cavaPluginManagerPtr, NULL);
    ASSERT_TRUE(ret);

    string configStr2 =  "{\
        \"summary_profile_name\": \"DefaultProfile\",\
        \"extractor\" :{\
            \"extractor_name\" : \"DefaultSummaryExtractor\",\
            \"module_name\" : \"BuildInModule\",\
            \"parameters\" : {\
                \"key\": \"value\"\
            }\
        },\
        \"field_configs\" : {\
            \"keywords\" : {\
                \"max_snippet\" : 1,\
                \"max_summary_length\" : 150,\
                \"abstract\" : true,\
                \"highlight_prefix\": \"<tag>\",\
                \"highlight_suffix\": \"</tag>\",\
                \"snippet_delimiter\": \"...\"\
            }\
        }\
    }";

    FromJsonString(summaryProfileInfo, configStr2);
    SummaryProfile summaryProfile2(summaryProfileInfo, _hitSummarySchemaPtr.get());
    ret = summaryProfile2.init(plugInManagerPtr, _resourceReaderPtr, _cavaPluginManagerPtr, NULL);
    ASSERT_TRUE(ret);
}
END_HA3_NAMESPACE(summary);
