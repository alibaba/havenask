#ifndef __INDEXLIB_OPERATIONWRITERTEST_H
#define __INDEXLIB_OPERATIONWRITERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"

IE_NAMESPACE_BEGIN(partition);

class OperationWriterTest : public INDEXLIB_TESTBASE
{
public:
    OperationWriterTest();
    ~OperationWriterTest();

    DECLARE_CLASS_NAME(OperationWriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestEstimateDumpSize();
    void TestDumpMetaFile();
    void TestCreateSegOpIterator();
    void TestCreateSegOpIteratorMultiThread();

private:
    void InnerTestDump(const std::string& docString,
                       const config::IndexPartitionSchemaPtr& schema);
    
    void AddOperations(const OperationWriterPtr& opWriter,
                       const std::string& docString,
                       size_t opCount,
                       int64_t startTs);

    void AddOperationThreadFunc(const OperationWriterPtr& opWriter,
                                const std::string& docString,
                                size_t opCount, int64_t startTs);
    
    void ReadOperationThreadFunc(const OperationWriterPtr& opWriter,
                                 size_t expectOpCount, int64_t startTs);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionSchemaPtr mMainSubSchema;
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OperationWriterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OperationWriterTest, TestEstimateDumpSize);
INDEXLIB_UNIT_TEST_CASE(OperationWriterTest, TestDumpMetaFile);
INDEXLIB_UNIT_TEST_CASE(OperationWriterTest, TestCreateSegOpIterator);
INDEXLIB_UNIT_TEST_CASE(OperationWriterTest, TestCreateSegOpIteratorMultiThread);


IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPERATIONWRITERTEST_H
