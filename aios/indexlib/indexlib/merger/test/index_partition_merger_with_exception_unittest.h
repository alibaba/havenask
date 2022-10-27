#ifndef __INDEXLIB_INDEXPARTITIONMERGERWITHEXCEPTIONTEST_H
#define __INDEXLIB_INDEXPARTITIONMERGERWITHEXCEPTIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/index_partition_merger.h"

IE_NAMESPACE_BEGIN(merger);

class IndexPartitionMergerWithExceptionTest : public INDEXLIB_TESTBASE
{
public:
    IndexPartitionMergerWithExceptionTest();
    ~IndexPartitionMergerWithExceptionTest();

    DECLARE_CLASS_NAME(IndexPartitionMergerWithExceptionTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMergeParallelWithException();
    void TestEndMergeException();
    void TestMultiPartitionEndMergeException();

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerWithExceptionTest,
                        TestMergeParallelWithException);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerWithExceptionTest,
                         TestEndMergeException);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionMergerWithExceptionTest,
                        TestMultiPartitionEndMergeException);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_INDEXPARTITIONMERGERWITHEXCEPTIONTEST_H
