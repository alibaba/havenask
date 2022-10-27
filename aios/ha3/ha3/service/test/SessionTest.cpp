#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/Session.h>
#include <ha3/service/SessionPool.h>

using namespace std;
BEGIN_HA3_NAMESPACE(service);

class SessionTest : public TESTBASE {
public:
    SessionTest();
    ~SessionTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, SessionTest);


SessionTest::SessionTest() {
}

SessionTest::~SessionTest() {
}

void SessionTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void SessionTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

class FakeSession: public Session
{
public:
    FakeSession(SessionPool *pool = NULL)
        : Session(pool) {}
    ~FakeSession() {}
public:
    void processRequest() {}
    void dropRequest() {destroy();}
public:
    void testDestroy() {
        return Session::destroy();
    }
};

TEST_F(SessionTest, testDestroy) {
    HA3_LOG(DEBUG, "Begin Test!");

    SessionPoolImpl<FakeSession> sessionPool(1);

    ASSERT_EQ((uint32_t)0, sessionPool.getCurrentCount());
    FakeSession* session1 = sessionPool.get();
    ASSERT_TRUE(!sessionPool.get());

    ASSERT_EQ((uint32_t)1, sessionPool.getCurrentCount());
    session1->testDestroy();
    ASSERT_EQ((uint32_t)0, sessionPool.getCurrentCount());
}

END_HA3_NAMESPACE(service);
