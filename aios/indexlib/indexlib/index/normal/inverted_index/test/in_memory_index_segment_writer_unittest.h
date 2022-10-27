#ifndef __INDEXLIB_MULTIINDEXWRITERTEST_H
#define __INDEXLIB_MULTIINDEXWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/in_memory_index_segment_writer.h"

IE_NAMESPACE_BEGIN(index);

class InMemoryIndexSegmentWriterTest : public INDEXLIB_TESTBASE
{
public:
    InMemoryIndexSegmentWriterTest();
    ~InMemoryIndexSegmentWriterTest();

    DECLARE_CLASS_NAME(InMemoryIndexSegmentWriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetAdjustHashMapInitSize();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemoryIndexSegmentWriterTest, TestGetAdjustHashMapInitSize);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTIINDEXWRITERTEST_H
