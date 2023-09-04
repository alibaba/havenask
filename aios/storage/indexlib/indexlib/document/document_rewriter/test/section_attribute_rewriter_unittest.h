#ifndef __INDEXLIB_SECTIONATTRIBUTEREWRITERTEST_H
#define __INDEXLIB_SECTIONATTRIBUTEREWRITERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/section_attribute_rewriter.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class SectionAttributeRewriterTest : public INDEXLIB_TESTBASE
{
public:
    SectionAttributeRewriterTest();
    ~SectionAttributeRewriterTest();

    DECLARE_CLASS_NAME(SectionAttributeRewriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRewriteWithNoSubDoc();
    void TestRewriteWithSubDoc();
    void TestRewriteWithException();
    void TestRewriteWithDeserializeDoc();

private:
    void InnerTestRewrite(const config::IndexPartitionSchemaPtr& schema, const std::string& docStr,
                          bool serializeDoc = false);

    config::IndexPartitionSchemaPtr CreateSchema(bool hasSubSchema, bool mainHasSectionAttr, bool subHasSectionAttr);

    void CheckDocument(const config::IndexPartitionSchemaPtr& schema, const document::NormalDocumentPtr& document);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SectionAttributeRewriterTest, TestRewriteWithNoSubDoc);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeRewriterTest, TestRewriteWithSubDoc);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeRewriterTest, TestRewriteWithException);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeRewriterTest, TestRewriteWithDeserializeDoc);
}} // namespace indexlib::document

#endif //__INDEXLIB_SECTIONATTRIBUTEREWRITERTEST_H
