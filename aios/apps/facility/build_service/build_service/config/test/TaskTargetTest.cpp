#include "build_service/config/TaskTarget.h"

#include "build_service/test/unittest.h"

using namespace std;
using namespace testing;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace build_service { namespace config {

class TaskTargetTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void TaskTargetTest::setUp() {}

void TaskTargetTest::tearDown() {}

TEST_F(TaskTargetTest, testSimple)
{
    TaskTarget target;
    target.setPartitionCount(1);
    target.setParallelNum(2);
    target.addTargetDescription("key", "value");

    // test jsonize and operator ==
    Any any = ToJson(target);
    TaskTarget compared;
    FromJson(compared, any);
    ASSERT_EQ(target, compared);
    ASSERT_EQ(1, compared.getPartitionCount());
    ASSERT_EQ(2, compared.getParallelNum());
    string value;
    ASSERT_TRUE(compared.getTargetDescription("key", value));
    ASSERT_EQ("value", value);
}

}} // namespace build_service::config
