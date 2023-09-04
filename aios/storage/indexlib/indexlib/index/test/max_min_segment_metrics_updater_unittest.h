#ifndef __INDEXLIB_DEFAULTSEGMENTATTRIBUTEUPDATERTEST_H
#define __INDEXLIB_DEFAULTSEGMENTATTRIBUTEUPDATERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/segment_metrics_updater/max_min_segment_metrics_updater.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class MaxMinSegmentMetricsUpdaterTest : public INDEXLIB_TESTBASE
{
public:
    MaxMinSegmentMetricsUpdaterTest();
    ~MaxMinSegmentMetricsUpdaterTest();

    DECLARE_CLASS_NAME(MaxMinSegmentMetricsUpdaterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MaxMinSegmentMetricsUpdaterTest, TestSimpleProcess);
}} // namespace indexlib::index

#endif //__INDEXLIB_DEFAULTSEGMENTATTRIBUTEUPDATERTEST_H
