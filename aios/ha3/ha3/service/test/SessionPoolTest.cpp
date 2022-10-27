#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/SessionPool.h>
#include <ha3/service/QrsArpcSession.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(service);

class SessionPoolTest : public TESTBASE {
public:
    SessionPoolTest();
    ~SessionPoolTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, SessionPoolTest);

using namespace autil;


SessionPoolTest::SessionPoolTest() { 
}

SessionPoolTest::~SessionPoolTest() { 
}

void SessionPoolTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    ::cava::CavaJit::globalInit();
}

void SessionPoolTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SessionPoolTest, testGetAndSetMaxSessionSize) { 
    HA3_LOG(DEBUG, "Begin Test!");
    SessionPoolImpl<QrsArpcSession> sessionPool;

    ASSERT_EQ((uint32_t)400, sessionPool.getMaxSessionSize());
    sessionPool.setMaxSessionSize(3);
    ASSERT_EQ((uint32_t)3, sessionPool.getMaxSessionSize());
    sessionPool.setMaxSessionSize(400); //avoid mem leak
 }

TEST_F(SessionPoolTest, testPutAndGetMethod) { 
    HA3_LOG(DEBUG, "Begin Test!");
    SessionPoolImpl<QrsArpcSession> sessionPool;

    ASSERT_EQ((uint32_t)400, sessionPool.getMaxSessionSize());
    sessionPool.setMaxSessionSize(3);
    ASSERT_EQ((uint32_t)3, sessionPool.getMaxSessionSize());

    ASSERT_EQ((uint32_t)0, sessionPool.getCurrentCount());
    Session* session1 = sessionPool.get(); 
    ASSERT_EQ((uint32_t)1, sessionPool.getCurrentCount());
    Session* session2 = sessionPool.get(); 
    ASSERT_EQ((uint32_t)2, sessionPool.getCurrentCount());
    Session* session3 = sessionPool.get(); 
    ASSERT_EQ((uint32_t)3, sessionPool.getCurrentCount());
    Session* session4 = sessionPool.get(); 
    ASSERT_TRUE(session4 == NULL);
    ASSERT_EQ((uint32_t)3, sessionPool.getCurrentCount());
    
    sessionPool.put(session1);
    ASSERT_EQ((uint32_t)2, sessionPool.getCurrentCount());
    sessionPool.put(session2);
    ASSERT_EQ((uint32_t)1, sessionPool.getCurrentCount());
    sessionPool.put(session3);
    ASSERT_EQ((uint32_t)0, sessionPool.getCurrentCount());
    sessionPool.setMaxSessionSize(400); //avoid mem leak
}

END_HA3_NAMESPACE(service);

