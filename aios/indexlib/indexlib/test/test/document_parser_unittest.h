#ifndef __INDEXLIB_DOCUMENTPARSERTEST_H
#define __INDEXLIB_DOCUMENTPARSERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/document_parser.h"

IE_NAMESPACE_BEGIN(test);

class DocumentParserTest : public INDEXLIB_TESTBASE
{
public:
    DocumentParserTest();
    ~DocumentParserTest();

    DECLARE_CLASS_NAME(DocumentParserTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocumentParserTest, TestSimpleProcess);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_DOCUMENTPARSERTEST_H
