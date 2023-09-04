#ifndef __INDEXLIB_PARTITIONSEGMENTITERATORTEST_H
#define __INDEXLIB_PARTITIONSEGMENTITERATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

class PartitionSegmentIteratorTest : public INDEXLIB_TESTBASE
{
public:
    PartitionSegmentIteratorTest();
    ~PartitionSegmentIteratorTest();

    DECLARE_CLASS_NAME(PartitionSegmentIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    static InMemorySegmentPtr CreateBuildingSegment(segmentid_t segId, int32_t docCount);
    static void CheckIterator(PartitionSegmentIterator& iter, const std::string& segInfoStr);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionSegmentIteratorTest, TestSimpleProcess);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_PARTITIONSEGMENTITERATORTEST_H
