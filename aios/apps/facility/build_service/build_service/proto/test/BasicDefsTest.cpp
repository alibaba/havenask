#include <iosfwd>
#include <stdint.h>

#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace proto {

class BasicDefsTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void BasicDefsTest::setUp() {}

void BasicDefsTest::tearDown() {}

TEST_F(BasicDefsTest, testRange)
{
    Range range;
    EXPECT_EQ(uint32_t(0), range.from());
    EXPECT_EQ(uint32_t(65535), range.to());
}

}} // namespace build_service::proto
