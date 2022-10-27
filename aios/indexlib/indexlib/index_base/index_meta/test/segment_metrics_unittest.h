#ifndef __INDEXLIB_SEGMENTMETRICSTEST_H
#define __INDEXLIB_SEGMENTMETRICSTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"

IE_NAMESPACE_BEGIN(index_base);

class SegmentMetricsTest : public INDEXLIB_TESTBASE
{
public:
    SegmentMetricsTest();
    ~SegmentMetricsTest();

    DECLARE_CLASS_NAME(SegmentMetricsTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestToString();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SegmentMetricsTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SegmentMetricsTest, TestToString);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENTMETRICSTEST_H
