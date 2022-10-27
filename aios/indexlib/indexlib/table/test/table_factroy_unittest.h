#ifndef __INDEXLIB_TABLEFACTROYTEST_H
#define __INDEXLIB_TABLEFACTROYTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/table/table_factory.h"

IE_NAMESPACE_BEGIN(table);

class TableFactroyTest : public INDEXLIB_TESTBASE
{
public:
    TableFactroyTest();
    ~TableFactroyTest();

    DECLARE_CLASS_NAME(TableFactroyTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleBuild();

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TableFactroyTest, TestSimpleBuild);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLEFACTROYTEST_H
