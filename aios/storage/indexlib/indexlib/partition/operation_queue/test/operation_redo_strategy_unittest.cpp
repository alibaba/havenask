#include "indexlib/partition/operation_queue/test/operation_redo_strategy_unittest.h"

#include "indexlib/config/online_config.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/operation_queue/test/mock_operation.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/partition_state_machine.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::index_base;
using namespace indexlib::test;
using namespace indexlib::config;
namespace indexlib { namespace partition {
IE_LOG_SETUP(index, OperationRedoStrategyTest);

OperationRedoStrategyTest::OperationRedoStrategyTest() {}

OperationRedoStrategyTest::~OperationRedoStrategyTest() {}

void OperationRedoStrategyTest::CaseSetUp() {}

void OperationRedoStrategyTest::CaseTearDown() {}

void OperationRedoStrategyTest::TestNeedRedo()
{
    InnerTestNeedRedo(true);
    InnerTestNeedRedo(false);
}

void OperationRedoStrategyTest::InnerTestNeedRedo(bool isIncConsistentWithRt)
{
    Version lastVersion(0, 50);
    lastVersion.AddSegment(0);
    lastVersion.AddSegment(1);
    lastVersion.AddSegment(7);
    lastVersion.AddSegment(8);

    Version version(1, 100);
    version.AddSegment(1);
    version.AddSegment(7);
    version.AddSegment(9);
    version.AddSegment(10);

    ReopenOperationRedoStrategy redoStrategy;
    redoStrategy.mIsIncConsistentWithRt = isIncConsistentWithRt;
    redoStrategy.InitSegmentMap(version, lastVersion);

    Pool pool;
    MockOperation* mockOperation = MockOperationMaker::MakeOperation(101, &pool);

    EXPECT_CALL(*mockOperation, GetDocOperateType()).WillRepeatedly(Return(UPDATE_FIELD));

    OperationRedoHint hint;
    if (isIncConsistentWithRt) {
        EXPECT_CALL(*mockOperation, GetSegmentId())
            .WillOnce(Return(1))                  // in version
            .WillOnce(Return(7))                  // in version
            .WillOnce(Return(0))                  // not in version, smaller than min segId
            .WillOnce(Return(3))                  // not in version
            .WillOnce(Return(8))                  // not in version
            .WillOnce(Return(9))                  // not in version
            .WillOnce(Return(10))                 // not in version
            .WillOnce(Return(INVALID_SEGMENTID)); // INVALID_SEGMENTID

        EXPECT_FALSE(redoStrategy.NeedRedo(1, mockOperation, hint));
        EXPECT_FALSE(redoStrategy.NeedRedo(1, mockOperation, hint));
        EXPECT_TRUE(redoStrategy.NeedRedo(1, mockOperation, hint));
        EXPECT_TRUE(redoStrategy.NeedRedo(1, mockOperation, hint));
        EXPECT_TRUE(redoStrategy.NeedRedo(1, mockOperation, hint));
        EXPECT_TRUE(redoStrategy.NeedRedo(1, mockOperation, hint));
        EXPECT_TRUE(redoStrategy.NeedRedo(1, mockOperation, hint));
        EXPECT_TRUE(redoStrategy.NeedRedo(1, mockOperation, hint));
    } else {
        EXPECT_TRUE(redoStrategy.NeedRedo(1, mockOperation, hint));
    }
    static_cast<OperationBase*>(mockOperation)->~OperationBase();
}

void OperationRedoStrategyTest::TestNeedRedoSpecialCondition()
{
    Version lastVersion(0, 50);
    lastVersion.AddSegment(0);
    lastVersion.AddSegment(1);
    lastVersion.AddSegment(7);

    Version version(1, 100);
    version.AddSegment(1);
    version.AddSegment(7);
    version.AddSegment(8);
    ReopenOperationRedoStrategy redoStrategy;
    redoStrategy.InitSegmentMap(version, lastVersion);
    redoStrategy.mMaxTsInNewVersion = 100;
    redoStrategy.mIsIncConsistentWithRt = true;
    Pool pool;
    OperationRedoHint hint;
    {
        MockOperation* mockOperation = MockOperationMaker::MakeOperation(101, &pool);
        EXPECT_CALL(*mockOperation, GetDocOperateType()).WillRepeatedly(Return(DELETE_DOC));

        EXPECT_CALL(*mockOperation, GetSegmentId()).WillOnce(Return(1));

        EXPECT_FALSE(redoStrategy.NeedRedo(1, mockOperation, hint));
        static_cast<OperationBase*>(mockOperation)->~OperationBase();
        const auto& skipDeleteSegSet = redoStrategy.GetSkipDeleteSegments();
        auto it = skipDeleteSegSet.find(1);
        EXPECT_TRUE(it != skipDeleteSegSet.end());
    }
    {
        MockOperation* mockOperation = MockOperationMaker::MakeOperation(101, &pool);
        EXPECT_CALL(*mockOperation, GetDocOperateType()).WillRepeatedly(Return(DELETE_DOC));

        EXPECT_CALL(*mockOperation, GetSegmentId()).WillOnce(Return(2));

        EXPECT_TRUE(redoStrategy.NeedRedo(1, mockOperation, hint));
        static_cast<OperationBase*>(mockOperation)->~OperationBase();
    }
    {
        MockOperation* mockOperation = MockOperationMaker::MakeOperation(101, &pool);
        EXPECT_CALL(*mockOperation, GetDocOperateType()).WillRepeatedly(Return(DELETE_SUB_DOC));
        EXPECT_TRUE(redoStrategy.NeedRedo(1, mockOperation, hint));
        static_cast<OperationBase*>(mockOperation)->~OperationBase();
    }
    {
        ReopenOperationRedoStrategy redoStrategy1;
        redoStrategy1.InitSegmentMap(version, lastVersion);
        redoStrategy1.mMaxTsInNewVersion = 100;
        redoStrategy1.mIsIncConsistentWithRt = true;
        redoStrategy1.mHasSubSchema = true;
        MockOperation* mockOperation = MockOperationMaker::MakeOperation(101, &pool);
        EXPECT_CALL(*mockOperation, GetDocOperateType()).WillRepeatedly(Return(DELETE_DOC));

        EXPECT_CALL(*mockOperation, GetSegmentId()).WillRepeatedly(Return(1));
        EXPECT_TRUE(redoStrategy1.NeedRedo(1, mockOperation, hint));
        static_cast<OperationBase*>(mockOperation)->~OperationBase();
    }
    {
        MockOperation* mockOperation = MockOperationMaker::MakeOperation(100, &pool);
        EXPECT_CALL(*mockOperation, GetDocOperateType()).WillRepeatedly(Return(DELETE_DOC));
        EXPECT_TRUE(redoStrategy.NeedRedo(1, mockOperation, hint));
        static_cast<OperationBase*>(mockOperation)->~OperationBase();
    }
    {
        MockOperation* mockOperation = MockOperationMaker::MakeOperation(100, &pool);
        EXPECT_CALL(*mockOperation, GetDocOperateType()).WillRepeatedly(Return(DELETE_DOC));
        EXPECT_TRUE(redoStrategy.NeedRedo(1, mockOperation, hint));
        static_cast<OperationBase*>(mockOperation)->~OperationBase();
    }
    {
        MockOperation* mockOperation = MockOperationMaker::MakeOperation(110, &pool);
        EXPECT_CALL(*mockOperation, GetDocOperateType()).WillRepeatedly(Return(UPDATE_FIELD));
        EXPECT_CALL(*mockOperation, GetSegmentId()).WillOnce(Return(1));
        EXPECT_FALSE(redoStrategy.NeedRedo(1, mockOperation, hint));
        static_cast<OperationBase*>(mockOperation)->~OperationBase();
    }
    {
        Pool pool;
        MockOperation* mockOperation = MockOperationMaker::MakeOperation(110, &pool);
        EXPECT_CALL(*mockOperation, GetDocOperateType()).WillRepeatedly(Return(UPDATE_FIELD));
        EXPECT_CALL(*mockOperation, GetSegmentId()).WillOnce(Return(0));
        EXPECT_TRUE(redoStrategy.NeedRedo(1, mockOperation, hint));
        static_cast<OperationBase*>(mockOperation)->~OperationBase();
    }
    {
        MockOperation* mockOperation = MockOperationMaker::MakeOperation(99, &pool);
        EXPECT_CALL(*mockOperation, GetDocOperateType()).WillRepeatedly(Return(UPDATE_FIELD));
        EXPECT_TRUE(redoStrategy.NeedRedo(1, mockOperation, hint));
        static_cast<OperationBase*>(mockOperation)->~OperationBase();
    }
}

void OperationRedoStrategyTest::TestInit()
{
    string field = "string1:string;";
    string index = "pk:primarykey64:string1";
    auto schema = SchemaMaker::MakeSchema(field, index, "", "");
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 1;
    PartitionStateMachine psm;
    psm.Init(schema, options, GET_TEMP_DATA_PATH());
    string fullDocString = "cmd=add,string1=pk0,ts=8,locator=0:9;"
                           "cmd=add,string1=pk1,ts=11,locator=0:7;"
                           "cmd=add,string1=pk2,ts=10,locator=0:9;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "pk:pk1", "docid=1;"));
    const auto& indexPart = psm.GetIndexPartition();
    auto partitionData = indexPart->GetPartitionData();
    {
        Version lastVersion(0, 100);
        lastVersion.SetFormatVersion(1);
        lastVersion.AddSegment(0);
        lastVersion.AddSegment(1);
        lastVersion.AddSegment(2);
        ReopenOperationRedoStrategy redoStrategy;
        OnlineConfig onlineConfig;
        onlineConfig.isIncConsistentWithRealtime = true;
        onlineConfig.SetEnableRedoSpeedup(true);
        onlineConfig.SetVersionTsAlignment(1);
        redoStrategy.Init(partitionData, lastVersion, onlineConfig, schema);
        EXPECT_FALSE(redoStrategy.mIsIncConsistentWithRt);
        EXPECT_EQ(11, redoStrategy.mMaxTsInNewVersion);
        EXPECT_EQ(4, redoStrategy.mBitmap.Size());
        EXPECT_FALSE(redoStrategy.mHasSubSchema);
    }
    {
        Version lastVersion(0, 100);
        lastVersion.SetFormatVersion(1);
        lastVersion.AddSegment(0);
        lastVersion.AddSegment(1);
        lastVersion.AddSegment(2);
        ReopenOperationRedoStrategy redoStrategy;
        OnlineConfig onlineConfig;
        onlineConfig.SetVersionTsAlignment(1);
        onlineConfig.isIncConsistentWithRealtime = true;
        redoStrategy.Init(partitionData, lastVersion, onlineConfig, schema);
        EXPECT_FALSE(redoStrategy.mIsIncConsistentWithRt);
        EXPECT_EQ(-1, redoStrategy.mMaxTsInNewVersion);
        EXPECT_EQ(0, redoStrategy.mBitmap.Size());
        EXPECT_FALSE(redoStrategy.mHasSubSchema);
    }
    {
        Version lastVersion(0, 100);
        lastVersion.SetFormatVersion(2);
        lastVersion.AddSegment(0);
        lastVersion.AddSegment(1);
        lastVersion.AddSegment(2);
        ReopenOperationRedoStrategy redoStrategy;
        OnlineConfig onlineConfig;
        onlineConfig.SetVersionTsAlignment(1);
        onlineConfig.isIncConsistentWithRealtime = true;
        onlineConfig.SetEnableRedoSpeedup(true);
        redoStrategy.Init(partitionData, lastVersion, onlineConfig, schema);
        EXPECT_TRUE(redoStrategy.mIsIncConsistentWithRt);
        EXPECT_EQ(11, redoStrategy.mMaxTsInNewVersion);
        EXPECT_EQ(3, redoStrategy.mBitmap.GetItemCount());
        EXPECT_EQ(3, redoStrategy.mBitmap.GetSetCount());
        EXPECT_TRUE(redoStrategy.mBitmap.Test(0));
        EXPECT_TRUE(redoStrategy.mBitmap.Test(1));
        EXPECT_TRUE(redoStrategy.mBitmap.Test(2));
        EXPECT_FALSE(redoStrategy.mHasSubSchema);
    }
    {
        Version lastVersion(0, 100);
        lastVersion.SetFormatVersion(2);
        lastVersion.AddSegment(0);
        lastVersion.AddSegment(1);
        lastVersion.AddSegment(2);
        ReopenOperationRedoStrategy redoStrategy;
        OnlineConfig onlineConfig;
        onlineConfig.SetVersionTsAlignment(1);
        onlineConfig.isIncConsistentWithRealtime = false;
        onlineConfig.SetEnableRedoSpeedup(true);
        redoStrategy.Init(partitionData, lastVersion, onlineConfig, schema);
        EXPECT_FALSE(redoStrategy.mIsIncConsistentWithRt);
        EXPECT_EQ(11, redoStrategy.mMaxTsInNewVersion);
        EXPECT_EQ(3, redoStrategy.mBitmap.GetItemCount());
        EXPECT_EQ(3, redoStrategy.mBitmap.GetSetCount());
        EXPECT_TRUE(redoStrategy.mBitmap.Test(0));
        EXPECT_TRUE(redoStrategy.mBitmap.Test(1));
        EXPECT_TRUE(redoStrategy.mBitmap.Test(2));
        EXPECT_FALSE(redoStrategy.mHasSubSchema);
    }
    {
        Version lastVersion(0, 100);
        lastVersion.SetFormatVersion(2);
        lastVersion.AddSegment(0);
        lastVersion.AddSegment(1);
        lastVersion.AddSegment(2);
        lastVersion.SetTimestamp(1574681753000001);
        ReopenOperationRedoStrategy redoStrategy;
        OnlineConfig onlineConfig;
        onlineConfig.isIncConsistentWithRealtime = false;
        onlineConfig.SetEnableRedoSpeedup(true);
        redoStrategy.Init(partitionData, lastVersion, onlineConfig, schema);
        EXPECT_FALSE(redoStrategy.mEnableRedoSpeedup);
    }
}
}} // namespace indexlib::partition
