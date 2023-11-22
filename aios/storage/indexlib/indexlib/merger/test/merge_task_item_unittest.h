#pragma once

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_task_item.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class MergeTaskItemTest : public INDEXLIB_TESTBASE
{
public:
    MergeTaskItemTest();
    ~MergeTaskItemTest();

    DECLARE_CLASS_NAME(MergeTaskItemTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestGetCheckPointName();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeTaskItemTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MergeTaskItemTest, TestGetCheckPointName);
}} // namespace indexlib::merger
