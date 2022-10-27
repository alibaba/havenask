#ifndef __INDEXLIB_SERIALIZEDSUMMARYDOCUMENTTEST_H
#define __INDEXLIB_SERIALIZEDSUMMARYDOCUMENTTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/index_document/normal_document/serialized_summary_document.h"

IE_NAMESPACE_BEGIN(document);

class SerializedSummaryDocumentTest : public INDEXLIB_TESTBASE
{
public:
    SerializedSummaryDocumentTest();
    ~SerializedSummaryDocumentTest();

    DECLARE_CLASS_NAME(SerializedSummaryDocumentTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSerialize();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SerializedSummaryDocumentTest, TestSerialize);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_SERIALIZEDSUMMARYDOCUMENTTEST_H
