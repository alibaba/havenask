#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

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
    void TestRewriteDisableSummary();
    void TestRewriteDisableSource();
    void TestRewriteBloomFilterParamForPKIndex();
    void TestEnableBloomFilterForIndexDictionary();
    void TestRewriteBuildParallelLookupPK();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestDisableFields);
INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestDisableAttributeInSummary);
INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestRewriteWithModifyOperation);
INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestRewriteFieldType);
INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestRewriteDisableSummary);
INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestRewriteDisableSource);
INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestRewriteBloomFilterParamForPKIndex);
INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestEnableBloomFilterForIndexDictionary);
INDEXLIB_UNIT_TEST_CASE(SchemaRewriterTest, TestRewriteBuildParallelLookupPK);

}} // namespace indexlib::index_base
