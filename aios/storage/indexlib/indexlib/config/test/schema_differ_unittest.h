#ifndef __INDEXLIB_SCHEMADIFFERTEST_H
#define __INDEXLIB_SCHEMADIFFERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/schema_differ.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace config {

class SchemaDifferTest : public INDEXLIB_TESTBASE
{
public:
    SchemaDifferTest();
    ~SchemaDifferTest();

    DECLARE_CLASS_NAME(SchemaDifferTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestAddSummary();
    void TestRemoveField();
    void TestWrongTableType();
    void TestTruncateIndex();
    void TestSubIndexSchema();
    void TestCustomizedIndex();
    void TestPackAttribute();
    void TestPrimaryKeyIndex();
    void TestRemoveFieldFail();
    void TestModifySchemaOperation();
    void TestUpdateFieldInModifySchema();
    void TestUpdateFieldInModifySchemaWithSubSchema();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SchemaDifferTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SchemaDifferTest, TestAddSummary);
INDEXLIB_UNIT_TEST_CASE(SchemaDifferTest, TestRemoveField);
INDEXLIB_UNIT_TEST_CASE(SchemaDifferTest, TestRemoveFieldFail);
INDEXLIB_UNIT_TEST_CASE(SchemaDifferTest, TestWrongTableType);
INDEXLIB_UNIT_TEST_CASE(SchemaDifferTest, TestTruncateIndex);
INDEXLIB_UNIT_TEST_CASE(SchemaDifferTest, TestSubIndexSchema);
INDEXLIB_UNIT_TEST_CASE(SchemaDifferTest, TestCustomizedIndex);
INDEXLIB_UNIT_TEST_CASE(SchemaDifferTest, TestPackAttribute);
INDEXLIB_UNIT_TEST_CASE(SchemaDifferTest, TestPrimaryKeyIndex);
INDEXLIB_UNIT_TEST_CASE(SchemaDifferTest, TestModifySchemaOperation);
INDEXLIB_UNIT_TEST_CASE(SchemaDifferTest, TestUpdateFieldInModifySchema);
INDEXLIB_UNIT_TEST_CASE(SchemaDifferTest, TestUpdateFieldInModifySchemaWithSubSchema);
}} // namespace indexlib::config

#endif //__INDEXLIB_SCHEMADIFFERTEST_H
