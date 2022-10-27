#ifndef __INDEXLIB_DOCUMENTCREATORTEST_H
#define __INDEXLIB_DOCUMENTCREATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/document_creator.h"

IE_NAMESPACE_BEGIN(test);

class DocumentCreatorTest : public INDEXLIB_TESTBASE
{
public:
    DocumentCreatorTest();
    ~DocumentCreatorTest();

    DECLARE_CLASS_NAME(DocumentCreatorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCreateSection();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocumentCreatorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DocumentCreatorTest, TestCreateSection);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_DOCUMENTCREATORTEST_H
