#ifndef __INDEXLIB_LIFECYCLESEGMENTMETRICSUPDATERTEST_H
#define __INDEXLIB_LIFECYCLESEGMENTMETRICSUPDATERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/segment_metrics_updater/lifecycle_segment_metrics_updater.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class LifecycleSegmentMetricsUpdaterTest : public INDEXLIB_TESTBASE
{
public:
    LifecycleSegmentMetricsUpdaterTest();
    ~LifecycleSegmentMetricsUpdaterTest();

    DECLARE_CLASS_NAME(LifecycleSegmentMetricsUpdaterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LifecycleSegmentMetricsUpdaterTest, TestSimpleProcess);
}} // namespace indexlib::index

#endif //__INDEXLIB_LIFECYCLESEGMENTMETRICSUPDATERTEST_H
