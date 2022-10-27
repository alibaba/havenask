#include <unittest/unittest.h>
#include <ha3/worker/HaProtoJsonizer.h>
#include <ha3/proto/QrsService.pb.h>

using namespace std;
using namespace testing;
using namespace ::google::protobuf;
using namespace isearch::proto;

BEGIN_HA3_NAMESPACE(worker);

class HaProtoJsonizerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void HaProtoJsonizerTest::setUp() {
}

void HaProtoJsonizerTest::tearDown() {
}

TEST_F(HaProtoJsonizerTest, testFromString) {
    QrsRequest request;
    HaProtoJsonizer haProtoJsonizer;
    string jsonStr = "/config=cluster:mainse_searcher&&query=test";
    haProtoJsonizer.fromJson(jsonStr, &request);
    ASSERT_EQ(jsonStr, request.assemblyquery());

    jsonStr = "{\"assemblyQuery\":\"c\"}";
    haProtoJsonizer.fromJson(jsonStr, &request);
    ASSERT_EQ(string("c"), request.assemblyquery());
}

TEST_F(HaProtoJsonizerTest, testToString) {
    QrsResponse response;
    response.set_assemblyresult("ret");
    HaProtoJsonizer haProtoJsonizer;
    ASSERT_EQ(string("ret"), haProtoJsonizer.toJson(response));

    QrsRequest request;
    request.set_assemblyquery("test");
    string expect = "{\n\"assemblyQuery\":\n  \"test\"\n}";
    ASSERT_EQ(expect, haProtoJsonizer.toJson(request));
}

END_HA3_NAMESPACE();

