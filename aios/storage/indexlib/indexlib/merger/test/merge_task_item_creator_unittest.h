#ifndef __INDEXLIB_MERGETASKITEMCREATORTEST_H
#define __INDEXLIB_MERGETASKITEMCREATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_task_item_creator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class MergeTaskItemCreatorTest : public INDEXLIB_TESTBASE
{
public:
    MergeTaskItemCreatorTest();
    ~MergeTaskItemCreatorTest();

    DECLARE_CLASS_NAME(MergeTaskItemCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestParallelMerge();
    void TestInitSummaryMergeIdentify();
    void TestInitSourceMergeIdentify();
    void TestInitAttributeMergeIdentify();
    void TestInitIndexMergeIdentify();
    // TODO
private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeTaskItemCreatorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemCreatorTest, TestParallelMerge);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemCreatorTest, TestInitIndexMergeIdentify);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemCreatorTest, TestInitSummaryMergeIdentify);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemCreatorTest, TestInitSourceMergeIdentify);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemCreatorTest, TestInitAttributeMergeIdentify);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGETASKITEMCREATORTEST_H
