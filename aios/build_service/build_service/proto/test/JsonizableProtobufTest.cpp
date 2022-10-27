#include "build_service/test/unittest.h"
#include "build_service/proto/JsonizableProtobuf.h"
#include "build_service/proto/BasicDefs.pb.h"
using namespace std;
using namespace testing;

namespace build_service {
namespace proto {

class JsonizableProtobufTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void JsonizableProtobufTest::setUp() {
}

void JsonizableProtobufTest::tearDown() {
}

TEST_F(JsonizableProtobufTest, testSimple) {
    JsonizableProtobuf<PartitionId> pid;
    pid.get().set_role(ROLE_PROCESSOR);
    JsonizableProtobuf<PartitionId> pid2;
    FromJsonString(pid2, ToJsonString(pid));
    ASSERT_TRUE(pid == pid2);
}

}
}
