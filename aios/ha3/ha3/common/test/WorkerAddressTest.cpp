#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/WorkerAddress.h>

using namespace std;
BEGIN_HA3_NAMESPACE(common);

class WorkerAddressTest : public TESTBASE {
public:
    WorkerAddressTest();
    ~WorkerAddressTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, WorkerAddressTest);


WorkerAddressTest::WorkerAddressTest() { 
}

WorkerAddressTest::~WorkerAddressTest() { 
}

void WorkerAddressTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void WorkerAddressTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(WorkerAddressTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_EQ(string("xxx:1"), WorkerAddress("xxx:1").getAddress());
    ASSERT_EQ(string("xxx"), WorkerAddress("xxx:1").getIp());
    ASSERT_EQ(uint16_t(1), WorkerAddress("xxx:1").getPort());

    ASSERT_EQ(string("127.0.0.1:122"), WorkerAddress("127.0.0.1", 122).getAddress());
    ASSERT_EQ(string("127.0.0.1"), WorkerAddress("127.0.0.1", 122).getIp());
    ASSERT_EQ(uint16_t(122), WorkerAddress("127.0.0.1", 122).getPort());
}

END_HA3_NAMESPACE(common);

