#ifndef __INDEXLIB_OPERATIONWRITERPERFTEST_H
#define __INDEXLIB_OPERATIONWRITERPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/operation_queue/operation_writer.h"

IE_NAMESPACE_BEGIN(partition);

class OperationWriterPerfTest : public INDEXLIB_TESTBASE
{
public:
    OperationWriterPerfTest();
    ~OperationWriterPerfTest();

    DECLARE_CLASS_NAME(OperationWriterPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDumpPerf();

private:
    config::IndexPartitionSchemaPtr mSchema;
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OperationWriterPerfTest, TestDumpPerf);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPERATIONWRITERPERFTEST_H
