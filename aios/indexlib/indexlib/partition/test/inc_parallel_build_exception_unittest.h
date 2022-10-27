#ifndef __INDEXLIB_INCPARALLELBUILDEXCEPTIONTEST_H
#define __INDEXLIB_INCPARALLELBUILDEXCEPTIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_options.h"

IE_NAMESPACE_BEGIN(partition);

class IncParallelBuildExceptionTest : public INDEXLIB_TESTBASE
{
public:
    IncParallelBuildExceptionTest();
    ~IncParallelBuildExceptionTest();

    DECLARE_CLASS_NAME(IncParallelBuildExceptionTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    void InnerParallelBuild(const std::string& buildPath);
    void InnerMergeBuildIndex(const std::string& buildPath);
    
private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    std::vector<std::string> mMergeSrc;
    std::string mRootPath;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IncParallelBuildExceptionTest, TestSimpleProcess);

IE_NAMESPACE_END(partition);
#endif //__INDEXLIB_INCPARALLELBUILDEXCEPTIONTEST_H
