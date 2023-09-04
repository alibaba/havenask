#include "suez/sdk/SchedulerInfo.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iosfwd>
#include <string>

#include "autil/legacy/legacy_jsonizable.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class SchedulerInfoTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SchedulerInfoTest::setUp() {}

void SchedulerInfoTest::tearDown() {}

TEST_F(SchedulerInfoTest, testJsonizeAndParseFrom) {
    SchedulerInfo schedulerInfo;
    schedulerInfo._cm2TopoInfo = "cm2TopoInfo";
    string schedulerInfoStr = FastToJsonString(schedulerInfo);
    SchedulerInfo schedulerInfo2;
    ASSERT_TRUE(schedulerInfo2.parseFrom(schedulerInfoStr));
    ASSERT_EQ(schedulerInfo2._cm2TopoInfo, schedulerInfo._cm2TopoInfo);
    SchedulerInfo schedulerInfo3;
    ASSERT_TRUE(schedulerInfo3.parseFrom(""));
}

TEST_F(SchedulerInfoTest, testOperator) {
    SchedulerInfo schedulerInfo;
    schedulerInfo._cm2TopoInfo = "cm2TopoInfo";
    SchedulerInfo schedulerInfo2;
    schedulerInfo2._cm2TopoInfo = "cm2TopoInfo";
    ASSERT_TRUE(schedulerInfo == schedulerInfo2);
    schedulerInfo2._cm2TopoInfo = "cm2TopoInfo2";
    ASSERT_TRUE(schedulerInfo != schedulerInfo2);
}

TEST_F(SchedulerInfoTest, testInitialized) {
    SchedulerInfo schedulerInfo;
    string str = "{}";
    ASSERT_TRUE(schedulerInfo.parseFrom(str));
    string topoInfo;
    ASSERT_FALSE(schedulerInfo.getCm2TopoInfo(topoInfo));
    ASSERT_EQ("", topoInfo);
    str = "{\"cm2_topo_info\":\"\"}";
    ASSERT_TRUE(schedulerInfo.parseFrom(str));
    ASSERT_TRUE(schedulerInfo.getCm2TopoInfo(topoInfo));
    ASSERT_EQ("", topoInfo);
    str = "{\"cm2_topo_info\":\"table1\"}";
    ASSERT_TRUE(schedulerInfo.parseFrom(str));
    ASSERT_TRUE(schedulerInfo.getCm2TopoInfo(topoInfo));
    ASSERT_EQ("table1", topoInfo);
    str = "{}";
    ASSERT_TRUE(schedulerInfo.parseFrom(str));
    ASSERT_FALSE(schedulerInfo.getCm2TopoInfo(topoInfo));
    ASSERT_EQ("", topoInfo);
}

} // namespace suez
