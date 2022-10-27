#ifndef __INDEXLIB_ADDTOUPDATEDOCUMENTREWRITERTEST_H
#define __INDEXLIB_ADDTOUPDATEDOCUMENTREWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/document_rewriter/add_to_update_document_rewriter.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

IE_NAMESPACE_BEGIN(document);

class AddToUpdateDocumentRewriterTest : public INDEXLIB_TESTBASE
{
public:
    AddToUpdateDocumentRewriterTest();
    ~AddToUpdateDocumentRewriterTest();

    DECLARE_CLASS_NAME(AddToUpdateDocumentRewriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAddUpdatableFields();
    void TestFilterTruncateSortFields();
    void TestRewrite();
    void TestRewriteForPackFields();
private:
    void CheckDocumentModifiedFields(const document::NormalDocumentPtr& doc);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AddToUpdateDocumentRewriterTest, TestAddUpdatableFields);
INDEXLIB_UNIT_TEST_CASE(AddToUpdateDocumentRewriterTest, TestFilterTruncateSortFields);
INDEXLIB_UNIT_TEST_CASE(AddToUpdateDocumentRewriterTest, TestRewrite);
INDEXLIB_UNIT_TEST_CASE(AddToUpdateDocumentRewriterTest, TestRewriteForPackFields);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_ADDTOUPDATEDOCUMENTREWRITERTEST_H
