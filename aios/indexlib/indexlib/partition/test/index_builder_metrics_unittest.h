#ifndef __INDEXLIB_INDEXBUILDERMETRICSTEST_H
#define __INDEXLIB_INDEXBUILDERMETRICSTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/index_builder_metrics.h"

IE_NAMESPACE_BEGIN(partition);

class IndexBuilderMetricsTest : public INDEXLIB_TESTBASE
{
public:
    IndexBuilderMetricsTest();
    ~IndexBuilderMetricsTest();

    DECLARE_CLASS_NAME(IndexBuilderMetricsTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAddToUpdate();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexBuilderMetricsTest, TestAddToUpdate);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEXBUILDERMETRICSTEST_H
