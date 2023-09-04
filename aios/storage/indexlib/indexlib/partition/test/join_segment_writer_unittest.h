#ifndef __INDEXLIB_JOINSEGMENTWRITERTEST_H
#define __INDEXLIB_JOINSEGMENTWRITERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/join_segment_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class JoinSegmentWriterTest : public INDEXLIB_TESTBASE
{
public:
    JoinSegmentWriterTest();
    ~JoinSegmentWriterTest();

    DECLARE_CLASS_NAME(JoinSegmentWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    // todo: add a case ?
    // void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

// INDEXLIB_UNIT_TEST_CASE(JoinSegmentWriterTest, TestSimpleProcess);
}} // namespace indexlib::partition

#endif //__INDEXLIB_JOINSEGMENTWRITERTEST_H
