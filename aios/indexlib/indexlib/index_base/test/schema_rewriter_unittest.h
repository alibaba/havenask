#ifndef __INDEXLIB_SCHEMAREWRITERTEST_H
#define __INDEXLIB_SCHEMAREWRITERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/schema_rewriter.h"

IE_NAMESPACE_BEGIN(index_base);

class SchemaRewriterTest : public INDEXLIB_TESTBASE
{
public:
    SchemaRewriterTest();
    ~SchemaRewriterTest();

    DECLARE_CLASS_NAME(SchemaRewriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDisableFields();
    void TestDisableAttributeInSummary();
    void TestRewriteWithModifyOperation();
    void TestRewriteFieldType();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestDisableFields);
INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestDisableAttributeInSummary);
INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestRewriteWithModifyOperation);
INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestRewriteFieldType);

IE_NAMESPACE_END(index_base);
#endif //__INDEXLIB_SCHEMAREWRITERTEST_H
