#include "indexlib/table/index_task/SimpleIndexTaskPlanCreator.h"

#include "indexlib/framework/index_task/IndexTaskPlan.h"
#include "unittest/unittest.h"

namespace indexlibv2::table {

class SimpleIndexTaskPlanCreatorTest : public TESTBASE
{
public:
    SimpleIndexTaskPlanCreatorTest() = default;
    ~SimpleIndexTaskPlanCreatorTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

class SimpleIndexTaskPlanCreatorForTest : public SimpleIndexTaskPlanCreator
{
public:
    SimpleIndexTaskPlanCreatorForTest() : SimpleIndexTaskPlanCreator("taskName", "taskTraceId", /*params=*/ {}) {}
    std::string ConstructLogTaskType() const override { return ""; }
    std::string ConstructLogTaskId(const framework::IndexTaskContext* taskContext,
                                   const framework::Version& targetVersion) const override
    {
        return "";
    }
    std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
    CreateTaskPlan(const framework::IndexTaskContext* context) override
    {
        return {Status::OK(), nullptr};
    }
    bool TestNeedDaytimeTrigger(const struct tm& triggerTime, const int64_t lastTaskTsInSec) const
    {
        return NeedDaytimeTrigger(triggerTime, lastTaskTsInSec);
    }
};

void SimpleIndexTaskPlanCreatorTest::setUp() {}

void SimpleIndexTaskPlanCreatorTest::tearDown() {}

TEST_F(SimpleIndexTaskPlanCreatorTest, testNeedDaytimeTrigger)
{
    SimpleIndexTaskPlanCreatorForTest creator;
    struct tm triggerTime = {0};
    for (size_t hour = 0; hour < 24; ++hour) {
        for (size_t min = 0; min < 60; ++min) {
            triggerTime.tm_hour = hour;
            triggerTime.tm_min = min;
            EXPECT_TRUE(creator.TestNeedDaytimeTrigger(triggerTime, 0));
            EXPECT_FALSE(creator.TestNeedDaytimeTrigger(triggerTime, time(0) + 3600));
        }
    }
}

} // namespace indexlibv2::table
