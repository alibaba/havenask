#pragma once

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_strategy/align_version_merge_strategy.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class AlignVersionMergeStrategyTest : public INDEXLIB_TESTBASE
{
public:
    AlignVersionMergeStrategyTest();
    ~AlignVersionMergeStrategyTest();

    DECLARE_CLASS_NAME(AlignVersionMergeStrategyTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AlignVersionMergeStrategyTest, TestSimpleProcess);
}} // namespace indexlib::merger
