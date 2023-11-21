#pragma once

#include "indexlib/common_define.h"
#include "indexlib/merger/parallel_end_merge_executor.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class ParallelEndMergeExecutorTest : public INDEXLIB_TESTBASE
{
public:
    ParallelEndMergeExecutorTest();
    ~ParallelEndMergeExecutorTest();

    DECLARE_CLASS_NAME(ParallelEndMergeExecutorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestExtractParallelMergeTaskGroup();
    void TestSingleGroupParallelEndMerge();

private:
    // descStr: groupId,id,totalParallelCount;groupId,id,totalParallelCount;...
    std::vector<MergeTaskItems> MakeMergeTaskItemsVec(const std::vector<std::string>& descStrVec);

    // groupId,groupSize;...
    void CheckMergeTaskGroups(const std::vector<MergeTaskItems>& taskGroups, const std::string& groupInfo);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ParallelEndMergeExecutorTest, TestExtractParallelMergeTaskGroup);
INDEXLIB_UNIT_TEST_CASE(ParallelEndMergeExecutorTest, TestSingleGroupParallelEndMerge);
}} // namespace indexlib::merger
