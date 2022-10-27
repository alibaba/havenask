#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/RpcContextUtil.h>
#include <sap_eagle/RpcContextImpl.h>

using namespace std;

BEGIN_HA3_NAMESPACE(service);

class RpcContextUtilTest : public TESTBASE {
public:
    RpcContextUtilTest();
    ~RpcContextUtilTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, RpcContextUtilTest);


RpcContextUtilTest::RpcContextUtilTest() { 
}

RpcContextUtilTest::~RpcContextUtilTest() { 
}

void RpcContextUtilTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    sap::RpcContextFactoryImpl::init(NULL, NULL);    
}

void RpcContextUtilTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    sap::RpcContextFactoryImpl::release();    
}

TEST_F(RpcContextUtilTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_EQ(string("1:1:1:1"), RpcContextUtil::normAddr("tcp:1:1:1:1"));
    ASSERT_EQ(string("123:456:123:123"), RpcContextUtil::normAddr("udp:123:456:123:123"));
    ASSERT_EQ(string("123:456:123:123"), RpcContextUtil::normAddr("123:456:123:123"));
}

TEST_F(RpcContextUtilTest, testUserData) {
    auto rpcContext = RpcContext::fromMap(map<string, string>());
    rpcContext->putUserData(HA_RESERVED_METHOD, "HA_RESERVED_METHOD");
    rpcContext->putUserData(HA_RESERVED_PID, "HA_RESERVED_PID");
    auto newMap = rpcContext->toMap();
    auto rpcContext2 = RpcContext::fromMap(newMap);
    EXPECT_EQ("HA_RESERVED_METHOD", rpcContext2->getUserData(HA_RESERVED_METHOD));
    EXPECT_EQ("HA_RESERVED_PID", rpcContext2->getUserData(HA_RESERVED_PID));
}

END_HA3_NAMESPACE(service);

