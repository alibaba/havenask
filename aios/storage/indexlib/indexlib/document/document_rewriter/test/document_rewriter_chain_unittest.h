#ifndef __INDEXLIB_DOCUMENTREWRITERCHAINTEST_H
#define __INDEXLIB_DOCUMENTREWRITERCHAINTEST_H

#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/document_rewriter_chain.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class DocumentRewriterChainTest : public INDEXLIB_TESTBASE
{
public:
    DocumentRewriterChainTest();
    ~DocumentRewriterChainTest();

    DECLARE_CLASS_NAME(DocumentRewriterChainTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAddToUpdate();
    void TestAddTimestamp();
    void TestAppendSectionAttribute();
    void TestAppendPackAttributes();
    void TestForKKVDocument();

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionSchemaPtr mRtSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocumentRewriterChainTest, TestAddToUpdate);
INDEXLIB_UNIT_TEST_CASE(DocumentRewriterChainTest, TestAddTimestamp);
INDEXLIB_UNIT_TEST_CASE(DocumentRewriterChainTest, TestAppendSectionAttribute);
INDEXLIB_UNIT_TEST_CASE(DocumentRewriterChainTest, TestAppendPackAttributes);
INDEXLIB_UNIT_TEST_CASE(DocumentRewriterChainTest, TestForKKVDocument);
}} // namespace indexlib::document

#endif //__INDEXLIB_DOCUMENTREWRITERCHAINTEST_H
