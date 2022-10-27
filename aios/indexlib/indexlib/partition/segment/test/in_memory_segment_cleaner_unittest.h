#ifndef __INDEXLIB_INMEMORYSEGMENTCLEANERTEST_H
#define __INDEXLIB_INMEMORYSEGMENTCLEANERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/segment/in_memory_segment_cleaner.h"

IE_NAMESPACE_BEGIN(partition);

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

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INMEMORYSEGMENTCLEANERTEST_H
