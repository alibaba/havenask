#ifndef __INDEXLIB_INDEXPARTITIONOPTIONSTEST_H
#define __INDEXLIB_INDEXPARTITIONOPTIONSTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_options.h"

IE_NAMESPACE_BEGIN(config);

class IndexPartitionOptionsTest : public INDEXLIB_TESTBASE
{
public:
    IndexPartitionOptionsTest();
    ~IndexPartitionOptionsTest();

    DECLARE_CLASS_NAME(IndexPartitionOptionsTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestJsonize();
    void TestGetBuildConfig();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexPartitionOptionsTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionOptionsTest, TestGetBuildConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_INDEXPARTITIONOPTIONSTEST_H
