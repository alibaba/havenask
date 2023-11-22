#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/segment/multi_region_kkv_segment_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class MultiRegionKkvSegmentWriterTest : public INDEXLIB_TESTBASE
{
public:
    MultiRegionKkvSegmentWriterTest();
    ~MultiRegionKkvSegmentWriterTest();

    DECLARE_CLASS_NAME(MultiRegionKkvSegmentWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInit();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MultiRegionKkvSegmentWriterTest, TestInit);
}} // namespace indexlib::partition
