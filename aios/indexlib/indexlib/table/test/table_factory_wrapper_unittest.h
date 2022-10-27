#ifndef __INDEXLIB_TABLEFACTORYWRAPPERTEST_H
#define __INDEXLIB_TABLEFACTORYWRAPPERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/table/table_factory_wrapper.h"

IE_NAMESPACE_BEGIN(table);

class TableFactoryWrapperTest : public INDEXLIB_TESTBASE
{
public:
    TableFactoryWrapperTest();
    ~TableFactoryWrapperTest();

    DECLARE_CLASS_NAME(TableFactoryWrapperTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInitException();
    void TestInitNormal();

private:
    config::IndexPartitionSchemaPtr mSchema;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TableFactoryWrapperTest, TestInitException);
INDEXLIB_UNIT_TEST_CASE(TableFactoryWrapperTest, TestInitNormal);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLEFACTORYWRAPPERTEST_H
