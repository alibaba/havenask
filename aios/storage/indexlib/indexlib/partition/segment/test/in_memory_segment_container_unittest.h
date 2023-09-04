#ifndef __INDEXLIB_INMEMORYSEGMENTCONTAINERTEST_H
#define __INDEXLIB_INMEMORYSEGMENTCONTAINERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

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
}} // namespace indexlib::partition

#endif //__INDEXLIB_INMEMORYSEGMENTCONTAINERTEST_H
