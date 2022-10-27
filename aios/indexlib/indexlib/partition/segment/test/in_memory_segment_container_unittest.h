#ifndef __INDEXLIB_INMEMORYSEGMENTCONTAINERTEST_H
#define __INDEXLIB_INMEMORYSEGMENTCONTAINERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"

IE_NAMESPACE_BEGIN(partition);

class InMemorySegmentContainerTest : public INDEXLIB_TESTBASE
{
public:
    InMemorySegmentContainerTest();
    ~InMemorySegmentContainerTest();

    DECLARE_CLASS_NAME(InMemorySegmentContainerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemorySegmentContainerTest, TestSimpleProcess);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INMEMORYSEGMENTCONTAINERTEST_H
