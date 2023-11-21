#include "indexlib/table/kv_table/index_task/KVTableMergePlanCreator.h"

#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskContextCreator.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/framework/index_task/LocalExecuteEngine.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/FenceDirDeleteOperation.h"
#include "indexlib/table/index_task/merger/MergedSegmentMoveOperation.h"
#include "indexlib/table/index_task/merger/MergedVersionCommitOperation.h"
#include "indexlib/table/kv_table/KVTabletFactory.h"
#include "indexlib/table/kv_table/index_task/KVTableResourceCreator.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace table {

class KVTableMergePlanCreatorTest : public TESTBASE
{
public:
    KVTableMergePlanCreatorTest();
    ~KVTableMergePlanCreatorTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();

private:
    std::shared_ptr<config::TabletOptions> CreateMultiShardOptions();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.table, KVTableMergePlanCreatorTest);

KVTableMergePlanCreatorTest::KVTableMergePlanCreatorTest() {}

KVTableMergePlanCreatorTest::~KVTableMergePlanCreatorTest() {}

std::shared_ptr<config::TabletOptions> KVTableMergePlanCreatorTest::CreateMultiShardOptions()
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3
        }
    }
    } )";
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetIsOnline(true);
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(1 * 1024 * 1024);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyStr("shard_based");
    return tabletOptions;
}

TEST_F(KVTableMergePlanCreatorTest, TestSimpleProcess)
{
    std::string field = "string1:string;string2:string";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2");
    auto options = CreateMultiShardOptions();
    std::string docs1 = "cmd=add,string1=1,string2=abc,ts=1,locator=0:1;"
                        "cmd=add,string1=2,string2=cde,ts=2,locator=0:2;"
                        "cmd=add,string1=3,string2=fgh,ts=3,locator=0:3;"
                        "cmd=add,string1=4,string2=ijk,ts=4,locator=0:4;";
    std::string docs2 = "cmd=add,string1=1,string2=ijk,ts=5,locator=0:5;"
                        "cmd=add,string1=12,string2=Acde,ts=6,locator=0:6;"
                        "cmd=add,string1=3,string2=Afgh,ts=7,locator=0:7;"
                        "cmd=add,string1=14,string2=Aijk,ts=8,locator=0:8;"
                        "cmd=add,string1=15,string2=15,ts=9,locator=0:9;"
                        "cmd=delete,string1=15,ts=10,locator=0:10;"
                        "cmd=delete,string1=2,ts=13,locator=0:11;";
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, tabletSchema, options).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(docs1).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(docs2).IsOK());

    std::unique_ptr<framework::ITabletFactory> factory = std::make_unique<KVTabletFactory>();
    std::string remoteRoot = std::string(indexRoot.GetRemoteRoot());
    factory->Init(options, nullptr);
    auto context = framework::IndexTaskContextCreator()
                       .SetTabletName("test")
                       .SetTaskEpochId("1232344556")
                       .SetExecuteEpochId("119.6.6.6_10086_12345678909876")
                       .SetTabletFactory(factory.get())
                       .SetTabletOptions(options)
                       .SetTabletSchema(tabletSchema)
                       .AddSourceVersion(remoteRoot, mainHelper.GetCurrentVersion().GetVersionId())
                       .SetDestDirectory(remoteRoot)
                       .CreateContext();
    KVTableMergePlanCreator creator(/*taskName=*/"name", /*taskTraceId=*/"id", /*params=*/ {});
    ASSERT_TRUE(context->SetDesignateTask("merge", "name"));
    auto [status, taskPlan] = creator.CreateTaskPlan(context.get());
    ASSERT_EQ("merge", taskPlan->GetTaskType());
    ASSERT_EQ("name", taskPlan->GetTaskName());
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(taskPlan);
    const auto& allOps = taskPlan->GetOpDescs();
    ASSERT_EQ(allOps.size(), 6);
    ASSERT_EQ(allOps[0].GetId(), 0);
    ASSERT_EQ(allOps[0].GetType(), "index_merge_operation");
    ASSERT_EQ(allOps[1].GetId(), 1);
    ASSERT_EQ(allOps[1].GetType(), MergedSegmentMoveOperation::OPERATION_TYPE);
    const auto& depends = allOps[1].GetDepends();
    ASSERT_EQ(1u, depends.size());
    ASSERT_EQ(0u, depends[0]);
    ASSERT_EQ(allOps[2].GetId(), 2);
    ASSERT_EQ(allOps[2].GetType(), "index_merge_operation");
    ASSERT_EQ(allOps[3].GetId(), 3);
    ASSERT_EQ(allOps[3].GetType(), MergedSegmentMoveOperation::OPERATION_TYPE);
    const auto& depends1 = allOps[3].GetDepends();
    ASSERT_EQ(1u, depends1.size());
    ASSERT_EQ(2u, depends1[0]);
    ASSERT_EQ(allOps[4].GetId(), 4);
    ASSERT_EQ(allOps[4].GetType(), MergedVersionCommitOperation::OPERATION_TYPE);
    const auto& depends2 = allOps[4].GetDepends();
    ASSERT_EQ(2u, depends2.size());
    ASSERT_EQ(1u, depends2[0]);
    ASSERT_EQ(3u, depends2[1]);
    ASSERT_EQ(allOps[5].GetId(), 5);
    ASSERT_EQ(allOps[5].GetType(), FenceDirDeleteOperation::OPERATION_TYPE);
    const auto& depends3 = allOps[5].GetDepends();
    ASSERT_EQ(1u, depends3.size());
    ASSERT_EQ(4u, depends3[0]);

    auto [status1, resource] = context->GetResourceManager()->LoadResource(/*name*/ MERGE_PLAN, /*type*/ MERGE_PLAN);
    ASSERT_TRUE(status1.IsOK());
    auto mergePlan = dynamic_cast<MergePlan*>(resource.get());
    ASSERT_TRUE(mergePlan);

    const auto& targetVersion = mergePlan->GetTargetVersion();
    ASSERT_TRUE(targetVersion.IsValid());
    ASSERT_EQ(targetVersion.GetVersionId(), 0);
    ASSERT_EQ(targetVersion.GetLastSegmentId(), framework::Segment::PUBLIC_SEGMENT_ID_MASK | 1);
    ASSERT_EQ(targetVersion.GetSegmentCount(), 2);
    ASSERT_TRUE(targetVersion.HasSegment(0));
    ASSERT_TRUE(targetVersion.HasSegment(1));
    ASSERT_EQ(targetVersion.GetTimestamp(), 13);
    ASSERT_EQ(11, targetVersion.GetLocator().GetOffset().first);
    ASSERT_EQ(0, targetVersion.GetLocator().GetSrc());
    auto levelInfo = targetVersion.GetSegmentDescriptions()->GetLevelInfo();
    ASSERT_TRUE(levelInfo);
    ASSERT_EQ(3u, levelInfo->GetLevelCount());
    ASSERT_EQ(2u, levelInfo->GetShardCount());
    uint32_t levelIdx;
    uint32_t shardIdx;
    ASSERT_TRUE(levelInfo->FindPosition(0, levelIdx, shardIdx));
    ASSERT_EQ(2, levelIdx);
    ASSERT_EQ(0, shardIdx);
    ASSERT_TRUE(levelInfo->FindPosition(1, levelIdx, shardIdx));
    ASSERT_EQ(2, levelIdx);
    ASSERT_EQ(1, shardIdx);
}

}} // namespace indexlibv2::table
