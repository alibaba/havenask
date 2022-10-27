#ifndef __INDEXLIB_TIMESERIESSPLITSTRATEGYTEST_H
#define __INDEXLIB_TIMESERIESSPLITSTRATEGYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/config/index_partition_options.h"

IE_NAMESPACE_BEGIN(merger);

class TimeSeriesSplitStrategyTest : public INDEXLIB_TESTBASE
{
public:
    TimeSeriesSplitStrategyTest();
    ~TimeSeriesSplitStrategyTest();

    DECLARE_CLASS_NAME(TimeSeriesSplitStrategyTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestSplitWithSelectionMergeStrategy();
    void TestSplitWithSelectionMergeStrategyNoSplit();

private:
    config::IndexPartitionSchemaPtr mSchema;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TimeSeriesSplitStrategyTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(TimeSeriesSplitStrategyTest, TestSplitWithSelectionMergeStrategy);
INDEXLIB_UNIT_TEST_CASE(TimeSeriesSplitStrategyTest, TestSplitWithSelectionMergeStrategyNoSplit);
IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_TIMESERIESSPLITSTRATEGYTEST_H
