#ifndef __INDEXLIB_OPERATIONITERATOR_PERF_TEST_H
#define __INDEXLIB_OPERATIONITERATOR_PERF_TEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/operation_queue/operation_iterator.h"

IE_NAMESPACE_BEGIN(partition);

class OperationIteratorPerfTest : public INDEXLIB_TESTBASE
{
public:
    OperationIteratorPerfTest();
    ~OperationIteratorPerfTest();

    DECLARE_CLASS_NAME(OperationIteratorPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestLoadPerf();

private:
    config::IndexPartitionSchemaPtr mSchema;
        
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OperationIteratorPerfTest, TestLoadPerf);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPERATIONITERATOR_PERF_TEST_H
