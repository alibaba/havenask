#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/service/QrsRequest.h>

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(service);

class QrsRequestTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, QrsRequestTest);

void QrsRequestTest::setUp() {
}

void QrsRequestTest::tearDown() {
}

TEST_F(QrsRequestTest, testConstruct) {
    auto message = new suez::turing::GraphRequest();
    QrsRequestPtr qrsRequest(new QrsRequest("methodName", message, 30, make_shared<google::protobuf::Arena>()));
    EXPECT_EQ("methodName", qrsRequest->getMethodName());
    EXPECT_EQ(message, qrsRequest->_message);
    EXPECT_EQ(30u, qrsRequest->getTimeout());
}

TEST_F(QrsRequestTest, testSerializeMessage) {
    {
        QrsRequestPtr qrsRequest(new QrsRequest(
            "methodName", NULL, 30, make_shared<google::protobuf::Arena>()));
        EXPECT_FALSE(qrsRequest->serializeMessage());
    }
    {
        auto message = new suez::turing::GraphRequest();
        QrsRequestPtr qrsRequest(new QrsRequest(
            "methodName", message, 30, make_shared<google::protobuf::Arena>()));
        auto ret = qrsRequest->serializeMessage();
        EXPECT_EQ(message, ret);
    }
}

END_HA3_NAMESPACE();
