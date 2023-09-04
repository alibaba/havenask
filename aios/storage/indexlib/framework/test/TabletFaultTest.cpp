#include "indexlib/framework/TabletFault.h"

#include "unittest/unittest.h"

namespace indexlibv2::framework {
using namespace autil::legacy;

class TabletFaultTest : public TESTBASE
{
public:
    TabletFaultTest() = default;
    ~TabletFaultTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void TabletFaultTest::setUp() {}

void TabletFaultTest::tearDown() {}

TEST_F(TabletFaultTest, testUsage)
{
    std::string faultStr = "some doc lost";
    TabletFault fault(faultStr);
    int64_t lastTriggerTime = fault.GetLastTriggerTimeUs();
    int64_t firstTriggerTime = fault.GetFirstTriggerTimeUs();
    ASSERT_EQ(firstTriggerTime, lastTriggerTime);
    ASSERT_EQ(1, fault.GetTriggerCount());
    ASSERT_EQ(faultStr, fault.GetFault());

    fault.TriggerAgain();
    ASSERT_EQ(firstTriggerTime, fault.GetFirstTriggerTimeUs());
    ASSERT_NE(firstTriggerTime, fault.GetLastTriggerTimeUs());
    ASSERT_EQ(2, fault.GetTriggerCount());
    ASSERT_EQ(faultStr, fault.GetFault());

    TabletFault fault2;
    FromJsonString(fault2, ToJsonString(fault));
    ASSERT_EQ(fault, fault2);
}

TEST_F(TabletFaultTest, testTabletFaultManager)
{
    std::string faultStr = "some doc lost";
    TabletFaultManager faultMap;
    faultMap.TriggerFailure(faultStr);
    ASSERT_EQ(1, faultMap.GetFault(faultStr).value().GetTriggerCount());
    faultMap.TriggerFailure(faultStr);
    ASSERT_EQ(2, faultMap.GetFault(faultStr).value().GetTriggerCount());

    std::string faultStr2 = "some doc build failed";
    TabletFaultManager faultMap2;
    faultMap2.TriggerFailure(faultStr);
    faultMap2.TriggerFailure(faultStr2);
    auto firstTriggerTime = faultMap.GetFault(faultStr).value().GetFirstTriggerTimeUs();
    auto lastTriggerTime = faultMap2.GetFault(faultStr).value().GetLastTriggerTimeUs();
    ASSERT_EQ(2, faultMap2.GetFaultCount());

    faultMap.MergeFault(faultMap2);
    ASSERT_EQ(3, faultMap.GetFault(faultStr).value().GetTriggerCount());
    ASSERT_EQ(firstTriggerTime, faultMap.GetFault(faultStr).value().GetFirstTriggerTimeUs());
    ASSERT_EQ(lastTriggerTime, faultMap.GetFault(faultStr).value().GetLastTriggerTimeUs());
    ASSERT_EQ(1, faultMap.GetFault(faultStr2).value().GetTriggerCount());
}

} // namespace indexlibv2::framework
