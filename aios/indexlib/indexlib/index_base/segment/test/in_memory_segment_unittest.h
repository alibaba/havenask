#ifndef __INDEXLIB_INMEMORYSEGMENTTEST_H
#define __INDEXLIB_INMEMORYSEGMENTTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/segment/in_memory_segment.h"

IE_NAMESPACE_BEGIN(index_base);

class InMemorySegmentTest : public INDEXLIB_TESTBASE
{
public:
    InMemorySegmentTest();
    ~InMemorySegmentTest();

    DECLARE_CLASS_NAME(InMemorySegmentTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDump();
    void TestDumpForSub();

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemorySegmentTest, TestDump);
INDEXLIB_UNIT_TEST_CASE(InMemorySegmentTest, TestDumpForSub);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_INMEMORYSEGMENTTEST_H
