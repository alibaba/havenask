#ifndef __INDEXLIB_MEMORYSTATCOLLECTORTEST_H
#define __INDEXLIB_MEMORYSTATCOLLECTORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/memory_stat_collector.h"

DECLARE_REFERENCE_CLASS(test, PartitionStateMachine);

IE_NAMESPACE_BEGIN(partition);

class MemoryStatCollectorTest : public INDEXLIB_TESTBASE
{
public:
    MemoryStatCollectorTest();
    ~MemoryStatCollectorTest();

    DECLARE_CLASS_NAME(MemoryStatCollectorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void CheckMetrics(misc::MetricPtr targetSampler,
                      misc::MetricProviderPtr provider,
                      const std::string &name, const std::string &unit);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MemoryStatCollectorTest, TestSimpleProcess);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MEMORYSTATCOLLECTORTEST_H
