#ifndef __INDEXLIB_SUBDOCADDTOUPDATEDOCUMENTREWRITERTEST_H
#define __INDEXLIB_SUBDOCADDTOUPDATEDOCUMENTREWRITERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/document_rewriter/sub_doc_add_to_update_document_rewriter.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

IE_NAMESPACE_BEGIN(document);

class SubDocAddToUpdateDocumentRewriterTest : public INDEXLIB_TESTBASE
{
public:
    SubDocAddToUpdateDocumentRewriterTest();
    ~SubDocAddToUpdateDocumentRewriterTest();

    DECLARE_CLASS_NAME(SubDocAddToUpdateDocumentRewriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
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

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_SUBDOCADDTOUPDATEDOCUMENTREWRITERTEST_H
