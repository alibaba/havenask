#include "indexlib/partition/operation_queue/optimized_reopen_redo_strategy.h"
#include "indexlib/partition/operation_queue/test/mock_operation.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/unittest.h"

using namespace std;

namespace indexlib { namespace partition {
using namespace indexlib::test;

class OptimizedReopenRedoStrategyTest : public INDEXLIB_TESTBASE
{
public:
    OptimizedReopenRedoStrategyTest();
    ~OptimizedReopenRedoStrategyTest();

    DECLARE_CLASS_NAME(OptimizedReopenRedoStrategyTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OptimizedReopenRedoStrategyTest, TestSimpleProcess);
IE_LOG_SETUP(partition, OptimizedReopenRedoStrategyTest);

OptimizedReopenRedoStrategyTest::OptimizedReopenRedoStrategyTest() {}

OptimizedReopenRedoStrategyTest::~OptimizedReopenRedoStrategyTest() {}

void OptimizedReopenRedoStrategyTest::CaseSetUp() {}

void OptimizedReopenRedoStrategyTest::CaseTearDown() {}

void OptimizedReopenRedoStrategyTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");
    string field = "string1:string;string2:string";
    string index = "pk:primarykey64:string1";
    string attribute = "string2";
    auto schema = test::SchemaMaker::MakeSchema(field, index, attribute, "");
    config::IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 1;
    test::PartitionStateMachine psm;
    psm.Init(schema, options, GET_TEMP_DATA_PATH());
    string fullDocString = "cmd=add,string1=pk0,string2=str0,ts=8,locator=0:9;"
                           "cmd=add,string1=pk1,string2=str1,ts=11,locator=0:7;"
                           "cmd=add,string1=pk2,string2=str2,ts=10,locator=0:9;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "pk:pk1", "docid=1;"));
    string incDocString = "cmd=add,string1=pk3,ts=8,locator=0:9;"
                          "cmd=add,string1=pk4,string2=str4,ts=11,locator=0:7;"
                          "cmd=add,string1=pk5,string2=str5,ts=10,locator=0:9;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));
    const auto& indexPart = psm.GetIndexPartition();
    auto partitionData = indexPart->GetPartitionData();

    index_base::Version version(0, 100);
    version.SetFormatVersion(2);
    version.AddSegment(0);
    version.AddSegment(1);
    version.AddSegment(2);
    OptimizedReopenRedoStrategy redoStrategy;
    redoStrategy.Init(partitionData, version, schema);
    EXPECT_EQ(6, redoStrategy.mDeleteDocRange.size());
    EXPECT_EQ(3, redoStrategy.mUpdateDocRange.size());
    autil::mem_pool::Pool pool;
    vector<pair<int, int>> emptyRange;

    {
        MockOperation* deleteOp = MockOperationMaker::MakeOperation(101, &pool);
        EXPECT_CALL(*deleteOp, GetDocOperateType()).WillRepeatedly(Return(DELETE_DOC));
        OperationRedoHint hint;
        ASSERT_TRUE(redoStrategy.NeedRedo(10, deleteOp, hint));
        ASSERT_TRUE(hint.IsValid());
        ASSERT_EQ(OperationRedoHint::REDO_DOC_RANGE, hint.GetHintType());
        vector<pair<docid_t, docid_t>> expectedRanges;
        for (size_t i = 0; i < 6; i++) {
            expectedRanges.push_back(make_pair(i, i + 1));
        };
        ASSERT_EQ(expectedRanges, hint.GetCachedDocRange().first);
        ASSERT_EQ(emptyRange, hint.GetCachedDocRange().second);
        static_cast<OperationBase*>(deleteOp)->~OperationBase();
    }
    {
        MockOperation* deleteSubOp = MockOperationMaker::MakeOperation(101, &pool);
        EXPECT_CALL(*deleteSubOp, GetDocOperateType()).WillRepeatedly(Return(DELETE_SUB_DOC));
        OperationRedoHint hint;
        ASSERT_TRUE(redoStrategy.NeedRedo(10, deleteSubOp, hint));
        ASSERT_TRUE(hint.IsValid());
        ASSERT_EQ(OperationRedoHint::REDO_DOC_RANGE, hint.GetHintType());
        vector<pair<docid_t, docid_t>> expectedRanges;
        for (size_t i = 0; i < 6; i++) {
            expectedRanges.push_back(make_pair(i, i + 1));
        };
        ASSERT_EQ(expectedRanges, hint.GetCachedDocRange().first);
        ASSERT_EQ(emptyRange, hint.GetCachedDocRange().second);
        static_cast<OperationBase*>(deleteSubOp)->~OperationBase();
    }
    {
        MockOperation* updateOp = MockOperationMaker::MakeOperation(101, &pool);
        EXPECT_CALL(*updateOp, GetDocOperateType()).WillRepeatedly(Return(UPDATE_FIELD));
        OperationRedoHint hint;
        ASSERT_TRUE(redoStrategy.NeedRedo(10, updateOp, hint));
        ASSERT_TRUE(hint.IsValid());
        ASSERT_EQ(OperationRedoHint::REDO_DOC_RANGE, hint.GetHintType());
        vector<pair<docid_t, docid_t>> expectedRanges;
        for (size_t i = 3; i < 6; i++) {
            expectedRanges.push_back(make_pair(i, i + 1));
        };
        ASSERT_EQ(expectedRanges, hint.GetCachedDocRange().first);
        ASSERT_EQ(emptyRange, hint.GetCachedDocRange().second);
        static_cast<OperationBase*>(updateOp)->~OperationBase();
    }
}
}} // namespace indexlib::partition
