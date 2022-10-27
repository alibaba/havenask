#ifndef __INDEXLIB_TIMESERIESSEGMENTMETRICSUPDATERTEST_H
#define __INDEXLIB_TIMESERIESSEGMENTMETRICSUPDATERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(index);

class TimeSeriesSegmentMetricsUpdaterTest : public INDEXLIB_TESTBASE
{
public:
    TimeSeriesSegmentMetricsUpdaterTest();
    ~TimeSeriesSegmentMetricsUpdaterTest();

    DECLARE_CLASS_NAME(TimeSeriesSegmentMetricsUpdaterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TimeSeriesSegmentMetricsUpdaterTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TIME_SERIES_SEGMENT_METRICS_UPDATER_UNITTEST_H
