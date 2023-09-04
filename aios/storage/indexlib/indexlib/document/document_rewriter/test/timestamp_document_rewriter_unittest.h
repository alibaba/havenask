#ifndef __INDEXLIB_TIMESTAMPDOCUMENTREWRITERTEST_H
#define __INDEXLIB_TIMESTAMPDOCUMENTREWRITERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/timestamp_document_rewriter.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class TimestampDocumentRewriterTest : public INDEXLIB_TESTBASE
{
public:
    TimestampDocumentRewriterTest();
    ~TimestampDocumentRewriterTest();

    DECLARE_CLASS_NAME(TimestampDocumentRewriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRewrite();
    void TestRewriteDocAlreadyHasTimestampField();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TimestampDocumentRewriterTest, TestRewrite);
INDEXLIB_UNIT_TEST_CASE(TimestampDocumentRewriterTest, TestRewriteDocAlreadyHasTimestampField);
}} // namespace indexlib::document

#endif //__INDEXLIB_TIMESTAMPDOCUMENTREWRITERTEST_H
