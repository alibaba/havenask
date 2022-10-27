#ifndef __INDEXLIB_INDEXBUILDERTEST_H
#define __INDEXLIB_INDEXBUILDERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/index_builder.h"

IE_NAMESPACE_BEGIN(partition);

class IndexBuilderTest : public INDEXLIB_TESTBASE
{
public:
    IndexBuilderTest();
    ~IndexBuilderTest();

    DECLARE_CLASS_NAME(IndexBuilderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAddDocument();
    void TestGetLocatorInLatesetVersion();
    void TestUpdateDcoumentFailedWhenAsyncDump();
private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexBuilderTest, TestAddDocument);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderTest, TestGetLocatorInLatesetVersion);
INDEXLIB_UNIT_TEST_CASE(IndexBuilderTest, TestUpdateDcoumentFailedWhenAsyncDump);
IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEXBUILDERTEST_H
