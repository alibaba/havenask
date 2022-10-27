#ifndef __INDEXLIB_INDEXBUILDEREXCEPTIONTEST_H
#define __INDEXLIB_INDEXBUILDEREXCEPTIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/index_builder.h"

IE_NAMESPACE_BEGIN(partition);

class IndexBuilderExceptionTest : public INDEXLIB_TESTBASE
{
public:
    IndexBuilderExceptionTest();
    ~IndexBuilderExceptionTest();

    DECLARE_CLASS_NAME(IndexBuilderExceptionTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDumpSegmentFailed();
private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexBuilderExceptionTest, TestDumpSegmentFailed);


IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEXBUILDEREXCEPTIONTEST_H
