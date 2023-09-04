#include "indexlib/table/kv_table/index_task/KVTableMergeDescriptionCreator.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/IndexMergeOperation.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace table {

class KVTableMergeDescriptionCreatorTest : public TESTBASE
{
public:
    KVTableMergeDescriptionCreatorTest();
    ~KVTableMergeDescriptionCreatorTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.table, KVTableMergeDescriptionCreatorTest);

KVTableMergeDescriptionCreatorTest::KVTableMergeDescriptionCreatorTest() {}

KVTableMergeDescriptionCreatorTest::~KVTableMergeDescriptionCreatorTest() {}

TEST_F(KVTableMergeDescriptionCreatorTest, TestCreateOperationDesc)
{
    std::string field = "string1:string;string2:string";
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2");
    auto indexConfig = tabletSchema->GetIndexConfig("kv", "string1");
    auto mergePlan = std::make_shared<MergePlan>("test", "merge");
    SegmentMergePlan segMergePlan;
    segMergePlan.AddSrcSegment(1);
    segMergePlan.AddSrcSegment(2);
    framework::SegmentInfo segInfo;
    segMergePlan.AddTargetSegment(3, segInfo);
    mergePlan->AddMergePlan(segMergePlan);
    framework::Version version;
    version.AddSegment(3);
    auto segDesc = version.GetSegmentDescriptions();
    auto levelInfo = std::make_shared<framework::LevelInfo>();
    segDesc->SetLevelInfo(levelInfo);
    levelInfo->Init(framework::topo_hash_mod, 3, 2);
    levelInfo->levelMetas[2].segments[0] = 3;
    mergePlan->SetTargetVersion(version);
    auto ts = autil::TimeUtility::currentTimeInSeconds();
    KVTableMergeDescriptionCreator creator(tabletSchema);
    auto [status, opDesc] = creator.CreateIndexMergeOperationDescription(mergePlan, indexConfig, 1, 0);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(IndexMergeOperation::OPERATION_TYPE, opDesc.GetType());
    string indexName, indexType, planIdx;
    ASSERT_TRUE(opDesc.GetParameter(MERGE_INDEX_NAME, indexName));
    ASSERT_TRUE(opDesc.GetParameter(MERGE_INDEX_TYPE, indexType));
    ASSERT_TRUE(opDesc.GetParameter(MERGE_PLAN_INDEX, planIdx));
    ASSERT_EQ("string1", indexName);
    ASSERT_EQ("kv", indexType);
    ASSERT_EQ("0", planIdx);
    bool dropDeleteKey;
    ASSERT_TRUE(opDesc.GetParameter(index::DROP_DELETE_KEY, dropDeleteKey));
    ASSERT_TRUE(dropDeleteKey);
    int64_t curTime;
    ASSERT_TRUE(opDesc.GetParameter(index::CURRENT_TIME_IN_SECOND, curTime));
    ASSERT_TRUE(curTime >= ts);

    levelInfo->levelMetas[2].segments[0] = -1;
    levelInfo->levelMetas[1].segments[0] = 3;
    auto [status1, opDesc1] = creator.CreateIndexMergeOperationDescription(mergePlan, indexConfig, 1, 0);
    ASSERT_TRUE(status1.IsOK());
    ASSERT_TRUE(opDesc1.GetParameter(index::DROP_DELETE_KEY, dropDeleteKey));
    ASSERT_FALSE(dropDeleteKey);
}

}} // namespace indexlibv2::table
