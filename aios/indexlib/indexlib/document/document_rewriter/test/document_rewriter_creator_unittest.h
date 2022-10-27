#ifndef __INDEXLIB_DOCUMENTREWRITERCREATORTEST_H
#define __INDEXLIB_DOCUMENTREWRITERCREATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/document_rewriter/document_rewriter_creator.h"

IE_NAMESPACE_BEGIN(document);

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

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocumentRewriterCreatorTest, TestCreateTimestampRewriter);
INDEXLIB_UNIT_TEST_CASE(DocumentRewriterCreatorTest, TestCreateAdd2UpdateRewriter);
INDEXLIB_UNIT_TEST_CASE(DocumentRewriterCreatorTest, TestCreateAdd2UpdateRewriterWithMultiConfigs);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_DOCUMENTREWRITERCREATORTEST_H
