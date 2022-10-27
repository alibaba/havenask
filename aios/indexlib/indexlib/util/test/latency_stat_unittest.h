#ifndef __INDEXLIB_LATENCYSTATTEST_H
#define __INDEXLIB_LATENCYSTATTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/latency_stat.h"

IE_NAMESPACE_BEGIN(util);

class LatencyStatTest : public INDEXLIB_TESTBASE {
public:
    LatencyStatTest();
    ~LatencyStatTest();

    DECLARE_CLASS_NAME(LatencyStatTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestLargeDataSet();

private:
    LatencyStat stat;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LatencyStatTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(LatencyStatTest, TestLargeDataSet);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_LATENCYSTATTEST_H
