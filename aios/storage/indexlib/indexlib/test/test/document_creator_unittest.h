#ifndef __INDEXLIB_DOCUMENTCREATORTEST_H
#define __INDEXLIB_DOCUMENTCREATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace test {

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
    void TestCreateKKVDocument();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocumentCreatorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DocumentCreatorTest, TestCreateSection);
INDEXLIB_UNIT_TEST_CASE(DocumentCreatorTest, TestCreateKKVDocument);
}} // namespace indexlib::test

#endif //__INDEXLIB_DOCUMENTCREATORTEST_H
