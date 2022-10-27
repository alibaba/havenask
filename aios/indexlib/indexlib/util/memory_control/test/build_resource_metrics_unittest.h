#ifndef __INDEXLIB_BUILDRESOURCEMETRICSTEST_H
#define __INDEXLIB_BUILDRESOURCEMETRICSTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"

IE_NAMESPACE_BEGIN(util);

class BuildResourceMetricsTest : public INDEXLIB_TESTBASE
{
public:
    BuildResourceMetricsTest();
    ~BuildResourceMetricsTest();

    DECLARE_CLASS_NAME(BuildResourceMetricsTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BuildResourceMetricsTest, TestSimpleProcess);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BUILDRESOURCEMETRICSTEST_H
