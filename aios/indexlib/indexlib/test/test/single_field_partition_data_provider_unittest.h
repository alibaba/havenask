#ifndef __INDEXLIB_SINGLEFIELDPARTITIONDATAPROVIDERTEST_H
#define __INDEXLIB_SINGLEFIELDPARTITIONDATAPROVIDERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/single_field_partition_data_provider.h"

IE_NAMESPACE_BEGIN(test);

class SingleFieldPartitionDataProviderTest : public INDEXLIB_TESTBASE
{
public:
    SingleFieldPartitionDataProviderTest();
    ~SingleFieldPartitionDataProviderTest();

    DECLARE_CLASS_NAME(SingleFieldPartitionDataProviderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreatePkIndex();
    void TestCreateIndex();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SingleFieldPartitionDataProviderTest, TestCreatePkIndex);
INDEXLIB_UNIT_TEST_CASE(SingleFieldPartitionDataProviderTest, TestCreateIndex);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_SINGLEFIELDPARTITIONDATAPROVIDERTEST_H
