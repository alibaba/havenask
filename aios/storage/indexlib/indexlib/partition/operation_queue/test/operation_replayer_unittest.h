#ifndef __INDEXLIB_OPERATIONREPLAYERTEST_H
#define __INDEXLIB_OPERATIONREPLAYERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/partition/operation_queue/operation_replayer.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OperationReplayerTest : public INDEXLIB_TESTBASE
{
public:
    struct OperationTestMeta {
        OperationCursor opCursor;
        int64_t opTs;
    };
    typedef std::vector<OperationTestMeta> OperationTestMetas;

public:
    OperationReplayerTest();
    ~OperationReplayerTest();
    DECLARE_CLASS_NAME(OperationReplayerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestRedoFromCursor();
    void TestRedoFilterByTimestamp();
    void TestRedoStrategy();
    void TestRedoStrategyDisableRedoSpeedup();
    void TestRedoStrategyWithRemoveOperation();
    void TestRedoRmOpWithIncInconsistentWithRt();
    void TestOpTargetSegIdUpdated();

private:
    // normalOpStr: opCount1,opCount2,opCount3
    void InnerTestRedoFromCursor(const std::string& normalOpStr, int inMemOpCount, int64_t onDiskVersionTimestamp);

    index_base::PartitionDataPtr MakeData(test::PartitionStateMachine& psm, const std::string& normalOpStr,
                                          int inMemOpCount, OperationTestMetas& opTestMeta);

    void CheckRedo(int64_t onDiskVersionTimestamp, const index_base::PartitionDataPtr& partData,
                   const OperationCursor& opCursor, const OperationCursor& expectLastCursor, size_t expectRedoCount);

    size_t GetRedoOpCount(const OperationTestMetas& opTestMetas, size_t beginPos, int64_t onDiskVersionTimestamp);

private:
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OperationReplayerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OperationReplayerTest, TestRedoFromCursor);
INDEXLIB_UNIT_TEST_CASE(OperationReplayerTest, TestRedoFilterByTimestamp);
INDEXLIB_UNIT_TEST_CASE(OperationReplayerTest, TestRedoStrategy);
INDEXLIB_UNIT_TEST_CASE(OperationReplayerTest, TestRedoStrategyDisableRedoSpeedup);
INDEXLIB_UNIT_TEST_CASE(OperationReplayerTest, TestRedoStrategyWithRemoveOperation);
INDEXLIB_UNIT_TEST_CASE(OperationReplayerTest, TestRedoRmOpWithIncInconsistentWithRt);
INDEXLIB_UNIT_TEST_CASE(OperationReplayerTest, TestOpTargetSegIdUpdated);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OPERATIONREPLAYERTEST_H
