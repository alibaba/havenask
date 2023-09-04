#ifndef __INDEXLIB_DOCUMENTREWRITERCREATORTEST_H
#define __INDEXLIB_DOCUMENTREWRITERCREATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/document_rewriter_creator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class DocumentRewriterCreatorTest : public INDEXLIB_TESTBASE
{
public:
    DocumentRewriterCreatorTest();
    ~DocumentRewriterCreatorTest();

    DECLARE_CLASS_NAME(DocumentRewriterCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreateTimestampRewriter();
    void TestCreateAdd2UpdateRewriter();
    void TestCreateAdd2UpdateRewriterWithMultiConfigs();
    void TestCreateHashIdDocumentRewriter();

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocumentRewriterCreatorTest, TestCreateTimestampRewriter);
INDEXLIB_UNIT_TEST_CASE(DocumentRewriterCreatorTest, TestCreateAdd2UpdateRewriter);
INDEXLIB_UNIT_TEST_CASE(DocumentRewriterCreatorTest, TestCreateAdd2UpdateRewriterWithMultiConfigs);
INDEXLIB_UNIT_TEST_CASE(DocumentRewriterCreatorTest, TestCreateHashIdDocumentRewriter);
}} // namespace indexlib::document

#endif //__INDEXLIB_DOCUMENTREWRITERCREATORTEST_H
