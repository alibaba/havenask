#ifndef __INDEXLIB_MULTISEGMENTMETRICSUPDATERTEST_H
#define __INDEXLIB_MULTISEGMENTMETRICSUPDATERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/multi_segment_metrics_updater.h"

IE_NAMESPACE_BEGIN(index);

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

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTISEGMENTMETRICSUPDATERTEST_H
