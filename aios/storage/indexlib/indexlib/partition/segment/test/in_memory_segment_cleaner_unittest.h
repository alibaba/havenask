#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/segment/in_memory_segment_cleaner.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class InMemorySegmentCleanerTest : public INDEXLIB_TESTBASE
{
public:
    InMemorySegmentCleanerTest();
    ~InMemorySegmentCleanerTest();

    DECLARE_CLASS_NAME(InMemorySegmentCleanerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemorySegmentCleanerTest, TestSimpleProcess);
}} // namespace indexlib::partition
