#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/service/QrsResponse.h>
#include <suez/turing/proto/Search.pb.h>

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(service);

class QrsResponseTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, QrsResponseTest);

void QrsResponseTest::setUp() {
}

void QrsResponseTest::tearDown() {
}

TEST_F(QrsResponseTest, testDeserializeApp) {
    std::shared_ptr<google::protobuf::Arena> arena(new google::protobuf::Arena());
    QrsResponse response(arena);
    EXPECT_FALSE(response.deserializeApp());
    auto searchResponse =
        google::protobuf::Arena::CreateMessage<suez::turing::GraphResponse>(
            arena.get());
    EXPECT_FALSE(searchResponse->has_multicall_ec());
    response._message = searchResponse;
    EXPECT_TRUE(response.deserializeApp());

    response._message = new suez::turing::BizGraphResponse();
    EXPECT_FALSE(response.deserializeApp());
}

END_HA3_NAMESPACE();
