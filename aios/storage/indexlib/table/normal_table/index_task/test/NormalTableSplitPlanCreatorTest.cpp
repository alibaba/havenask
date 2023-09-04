#include "indexlib/table/normal_table/index_task/NormalTableSplitPlanCreator.h"

#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/mock/FakeSegment.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/config/SegmentGroupConfig.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace table {

class NormalTableSplitPlanCreatorTest : public TESTBASE
{
public:
    NormalTableSplitPlanCreatorTest();
    ~NormalTableSplitPlanCreatorTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.table, NormalTableSplitPlanCreatorTest);

NormalTableSplitPlanCreatorTest::NormalTableSplitPlanCreatorTest() {}

NormalTableSplitPlanCreatorTest::~NormalTableSplitPlanCreatorTest() {}

TEST_F(NormalTableSplitPlanCreatorTest, TestNeedTriggerTask)
{
    std::shared_ptr<config::TabletSchema> schema(new config::TabletSchema());
    std::shared_ptr<framework::ResourceMap> resourceMap(new framework::ResourceMap);
    std::shared_ptr<framework::TabletData> tabletData(new framework::TabletData("demo"));
    framework::Version version(0);
    ASSERT_TRUE(tabletData->Init(version, {}, resourceMap).IsOK());
    NormalTableSplitPlanCreator creator(/*taskName=*/"split1", /*params=*/ {});
    config::IndexTaskConfig config("split1", NormalTableSplitPlanCreator::TASK_TYPE, "period=1");
    framework::IndexTaskContext context;
    context.TEST_SetTabletData(tabletData);
    context.TEST_SetTabletSchema(schema);
    {
        // no group config, no need trigger
        auto [status, needTrigger] = creator.NeedTriggerTask(config, &context);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(needTrigger);
    }
    {
        // no merged segment, no need trigger
        SegmentGroupConfig groupConfig;
        groupConfig.TEST_SetGroups({"group1"});
        ASSERT_TRUE(schema->TEST_GetImpl()->SetRuntimeSetting(NORMAL_TABLE_GROUP_CONFIG_KEY, groupConfig, true));
        auto [status, needTrigger] = creator.NeedTriggerTask(config, &context);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(needTrigger);
    }
    {
        // need trigger
        std::shared_ptr<framework::TabletData> tabletData2(new framework::TabletData("demo"));
        framework::Version version2(0);
        version2.AddSegment(0);
        std::vector<std::shared_ptr<framework::Segment>> segments;
        std::shared_ptr<framework::Segment> segment(
            new framework::FakeSegment(framework::Segment::SegmentStatus::ST_BUILT));
        framework::SegmentMeta meta(0);
        segment->TEST_SetSegmentMeta(meta);
        segments.push_back(segment);
        ASSERT_TRUE(tabletData2->Init(version, segments, resourceMap).IsOK());
        context.TEST_SetTabletData(tabletData2);
        auto [status, needTrigger] = creator.NeedTriggerTask(config, &context);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(needTrigger);
    }
    {
        // invalid schema
        ASSERT_TRUE(schema->TEST_GetImpl()->SetRuntimeSetting(NORMAL_TABLE_GROUP_CONFIG_KEY, "12345", true));
        auto [status, needTrigger] = creator.NeedTriggerTask(config, &context);
        ASSERT_FALSE(status.IsOK());
    }
}

}} // namespace indexlibv2::table
