#pragma once

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

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
}} // namespace indexlib::index
