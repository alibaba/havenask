#include "build_service/proto/ProtoComparator.h"

#include <iosfwd>

#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace proto {

class ProtoComparatorTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void ProtoComparatorTest::setUp() {}

void ProtoComparatorTest::tearDown() {}

TEST_F(ProtoComparatorTest, testSimple)
{
    BuildId b1, b2;
    b1.set_datatable("d1");
    b1.set_generationid(1);
    b1.set_appname("app1");
    b2.set_datatable("d1");
    b2.set_generationid(1);
    b2.set_appname("app1");
    EXPECT_TRUE(b1 == b2);
    b2.set_appname("app2");
    EXPECT_TRUE(b1 != b2);
}

}} // namespace build_service::proto
