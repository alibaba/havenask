#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/ServiceDegradationConfig.h>
#include <ha3/test/test.h>
#include <suez/turing/common/FileUtil.h>
using namespace std;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(config);

class ServiceDegradationConfigTest : public TESTBASE {
public:
    ServiceDegradationConfigTest();
    ~ServiceDegradationConfigTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, ServiceDegradationConfigTest);


ServiceDegradationConfigTest::ServiceDegradationConfigTest() { 
}

ServiceDegradationConfigTest::~ServiceDegradationConfigTest() { 
}

void ServiceDegradationConfigTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ServiceDegradationConfigTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ServiceDegradationConfigTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string context = FileUtil::readFile(TEST_DATA_PATH"/service_degradation.json");
    ServiceDegradationConfig config;
    FromJsonString(config, context);
    ASSERT_EQ(uint32_t(100), config.condition.workerQueueSizeDegradeThreshold);
    ASSERT_EQ(uint32_t(10), config.condition.workerQueueSizeRecoverThreshold);
    ASSERT_EQ(int64_t(1000), config.condition.duration);
    ASSERT_EQ(uint32_t(5000), config.request.rankSize);
    ASSERT_EQ(uint32_t(200), config.request.rerankSize);
}

END_HA3_NAMESPACE(config);

