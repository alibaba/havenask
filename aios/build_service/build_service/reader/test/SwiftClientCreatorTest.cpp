#include "build_service/test/unittest.h"
#include "build_service/util/SwiftClientCreator.h"
#include "build_service/reader/test/MockSwiftClientCreator.h"

using namespace std;
using namespace testing;
using namespace autil;

SWIFT_USE_NAMESPACE(client);

using namespace build_service::reader;
using namespace build_service::proto;
using namespace build_service::util;
namespace build_service {
namespace config {

class SwiftClientCreatorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SwiftClientCreatorTest::setUp() {
}

void SwiftClientCreatorTest::tearDown() {
}

TEST_F(SwiftClientCreatorTest, testSimplee) {
    SwiftClientCreatorPtr creator(new NiceMockSwiftClientCreator);
    SwiftClient* client1 = creator->createSwiftClient("zf://root/", "maxBufferSize=3");
    SwiftClient* client2 = creator->createSwiftClient("zf://root/", "maxBufferSize=5");
    SwiftClient* client3 = creator->createSwiftClient("zf://root/", "maxBufferSize=3");
    ASSERT_EQ(client3, client1);
    ASSERT_NE(client3, client2);
}

}
}
