#pragma once

#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/add_to_update_document_rewriter.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

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
    void TestAddUpdatableFields2();
    void TestFilterTruncateSortFields();
    void TestRewrite();
    void TestRewriteForPackFields();
    void TestRewriteIndexUpdate();

private:
    void CheckDocumentModifiedFields(const document::NormalDocumentPtr& doc);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AddToUpdateDocumentRewriterTest, TestAddUpdatableFields);
INDEXLIB_UNIT_TEST_CASE(AddToUpdateDocumentRewriterTest, TestAddUpdatableFields2);
INDEXLIB_UNIT_TEST_CASE(AddToUpdateDocumentRewriterTest, TestFilterTruncateSortFields);
INDEXLIB_UNIT_TEST_CASE(AddToUpdateDocumentRewriterTest, TestRewrite);
INDEXLIB_UNIT_TEST_CASE(AddToUpdateDocumentRewriterTest, TestRewriteForPackFields);
INDEXLIB_UNIT_TEST_CASE(AddToUpdateDocumentRewriterTest, TestRewriteIndexUpdate);
}} // namespace indexlib::document
