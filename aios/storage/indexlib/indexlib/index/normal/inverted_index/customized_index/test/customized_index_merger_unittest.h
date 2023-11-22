#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_merger.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class CustomizedIndexMergerTest : public INDEXLIB_TESTBASE
{
public:
    CustomizedIndexMergerTest();
    ~CustomizedIndexMergerTest();

    DECLARE_CLASS_NAME(CustomizedIndexMergerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMultiOutputSegment();
    void TestParallelReduceMeta();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CustomizedIndexMergerTest, TestMultiOutputSegment);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexMergerTest, TestParallelReduceMeta);
}} // namespace indexlib::index
