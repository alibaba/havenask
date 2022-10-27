#ifndef __INDEXLIB_OPERATIONITERATORTEST_H
#define __INDEXLIB_OPERATIONITERATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/operation_queue/operation_iterator.h"

DECLARE_REFERENCE_CLASS(partition, OperationWriter);
DECLARE_REFERENCE_CLASS(config, OperationWriter);
IE_NAMESPACE_BEGIN(partition);

class OperationIteratorTest : public INDEXLIB_TESTBASE
{
public:
    OperationIteratorTest();
    ~OperationIteratorTest();

    DECLARE_CLASS_NAME(OperationIteratorTest);

private:
    config::IndexPartitionSchemaPtr mSchema;
    
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestIterateOperations();
    void TestRedoBreakAtWholeObseleteOperationSegment_20681011();
    
private:
    void InnerTestIterateOperations(
            const std::string& segInfoStr, size_t buildingOpCount,
            size_t opBlockSize, bool compressOperation,
            const OperationCursor& startCursor, int64_t skipTs,
            size_t expectOpCount, int64_t expectStartTs,
            const OperationCursor& expectLastCursor,
            bool hasDumpingSegment = false);
    
    OperationWriterPtr CreateOperationWriter(
            const config::IndexPartitionSchemaPtr& schema,
            size_t operationBlockSize,  bool compressOperation);
        
    index_base::PartitionDataPtr PreparePartitionData(
            const std::string& segInfoStr, size_t buildingOpCount,
            size_t operationBlockSize, bool compressOperation,
            bool hasDumpingSegment = false,
            const std::string& opTimeStampStr = "",
            uint32_t segmentStartTs = 0);

    void GetTimestampVectorForTargetSegments(
            const std::vector<int32_t>& segOpCounts,
            size_t buildingOpCount, const std::string& opTimeStampStr,
            std::vector<std::vector<uint32_t> > &segOpTsVec,
            std::vector<uint32_t> &buildingOpTsVec);
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OperationIteratorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OperationIteratorTest, TestIterateOperations);
INDEXLIB_UNIT_TEST_CASE(OperationIteratorTest, TestRedoBreakAtWholeObseleteOperationSegment_20681011);
IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPERATIONITERATORTEST_H
