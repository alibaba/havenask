#ifndef __INDEXLIB_SCHEMAADAPTERTEST_H
#define __INDEXLIB_SCHEMAADAPTERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/schema_adapter.h"

IE_NAMESPACE_BEGIN(index_base);

class SchemaAdapterTest : public INDEXLIB_TESTBASE
{
public:
    SchemaAdapterTest();
    ~SchemaAdapterTest();

    DECLARE_CLASS_NAME(SchemaAdapterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForJsonizeTableType();
    void TestLoadFromString();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SchemaAdapterTest, TestCaseForJsonizeTableType);
INDEXLIB_UNIT_TEST_CASE(SchemaAdapterTest, TestLoadFromString);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SCHEMAADAPTERTEST_H
