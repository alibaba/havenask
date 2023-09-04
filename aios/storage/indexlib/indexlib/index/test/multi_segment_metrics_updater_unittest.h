#ifndef __INDEXLIB_MULTISEGMENTMETRICSUPDATERTEST_H
#define __INDEXLIB_MULTISEGMENTMETRICSUPDATERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/segment_metrics_updater/multi_segment_metrics_updater.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class MultiSegmentMetricsUpdaterTest : public INDEXLIB_TESTBASE
{
public:
    MultiSegmentMetricsUpdaterTest();
    ~MultiSegmentMetricsUpdaterTest();

    DECLARE_CLASS_NAME(MultiSegmentMetricsUpdaterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MultiSegmentMetricsUpdaterTest, TestSimpleProcess);
}} // namespace indexlib::index

#endif //__INDEXLIB_MULTISEGMENTMETRICSUPDATERTEST_H
