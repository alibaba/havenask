#pragma once

#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/sub_doc_add_to_update_document_rewriter.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class SubDocAddToUpdateDocumentRewriterTest : public INDEXLIB_TESTBASE
{
public:
    SubDocAddToUpdateDocumentRewriterTest() {}
    ~SubDocAddToUpdateDocumentRewriterTest() {}

    DECLARE_CLASS_NAME(SubDocAddToUpdateDocumentRewriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override {}
    void TestRewriteMainDoc();
    void TestRewriteSubDoc();
    void TestRewriteMainAndSub();
    void TestNonAddDoc();
    void TestEmptyModifiedFields();
    void TestRewriteMainFail();
    void TestRewriteSubFail();
    void TestInvalidModifiedFields();

private:
    document::NormalDocumentPtr CreateDocument(std::string docStr);
    void AddModifiedToken(const document::NormalDocumentPtr& doc, fieldid_t fieldId);
    void CheckDocumentModifiedFields(const document::NormalDocumentPtr& doc);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionSchemaPtr mSubSchema;
    SubDocAddToUpdateDocumentRewriterPtr mRewriter;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SubDocAddToUpdateDocumentRewriterTest, TestRewriteMainDoc);
INDEXLIB_UNIT_TEST_CASE(SubDocAddToUpdateDocumentRewriterTest, TestRewriteSubDoc);
INDEXLIB_UNIT_TEST_CASE(SubDocAddToUpdateDocumentRewriterTest, TestRewriteMainAndSub);
INDEXLIB_UNIT_TEST_CASE(SubDocAddToUpdateDocumentRewriterTest, TestNonAddDoc);
INDEXLIB_UNIT_TEST_CASE(SubDocAddToUpdateDocumentRewriterTest, TestEmptyModifiedFields);
INDEXLIB_UNIT_TEST_CASE(SubDocAddToUpdateDocumentRewriterTest, TestRewriteMainFail);
INDEXLIB_UNIT_TEST_CASE(SubDocAddToUpdateDocumentRewriterTest, TestRewriteSubFail);
INDEXLIB_UNIT_TEST_CASE(SubDocAddToUpdateDocumentRewriterTest, TestInvalidModifiedFields);
}} // namespace indexlib::document
