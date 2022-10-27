#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/SearchOptimizerConfig.h>

using namespace std;
using namespace autil::legacy;

BEGIN_HA3_NAMESPACE(config);

class SearchOptimizerConfigTest : public TESTBASE {
public:
    SearchOptimizerConfigTest();
    ~SearchOptimizerConfigTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, SearchOptimizerConfigTest);


SearchOptimizerConfigTest::SearchOptimizerConfigTest() { 
}

SearchOptimizerConfigTest::~SearchOptimizerConfigTest() { 
}

void SearchOptimizerConfigTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void SearchOptimizerConfigTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SearchOptimizerConfigTest, testJsonize) {
    string jsonStr = "{\
    \"modules\" : [                             \
	{                                       \
	    \"module_name\": \"test_module\",           \
	    \"module_path\": \"libfakeoptimizer.so\",   \
	    \"pararmeters\": {                          \
	    }                                           \
	}                                               \
    ],                                                  \
    \"optimizers\" : [                                  \
	{                                               \
	    \"optimizer_name\": \"FakeOptimizer\",      \
	    \"module_name\": \"test_module1\",          \
	    \"pararmeters\":{                           \
	    }                                           \
	}                                               \
    ]                                                   \
}";
    SearchOptimizerConfig config;
    FromJsonString(config, jsonStr);

    const build_service::plugin::ModuleInfos &moduleInfos = config.getModuleInfos();
    const OptimizerConfigInfos &optimizerConfigInfos = config.getOptimizerConfigInfos();

    ASSERT_EQ(size_t(1), moduleInfos.size());
    ASSERT_EQ(size_t(1), optimizerConfigInfos.size());

    ASSERT_EQ(string("test_module"), moduleInfos[0].moduleName);
    ASSERT_EQ(string("libfakeoptimizer.so"), moduleInfos[0].modulePath);

    ASSERT_EQ(string("FakeOptimizer"), optimizerConfigInfos[0].optimizerName);
    ASSERT_EQ(string("test_module1"), optimizerConfigInfos[0].moduleName);
}

END_HA3_NAMESPACE(config);

