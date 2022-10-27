#ifndef __INDEXLIB_ATTRIBUTEDOCUMENTTEST_H
#define __INDEXLIB_ATTRIBUTEDOCUMENTTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"

IE_NAMESPACE_BEGIN(document);

class AttributeDocumentTest : public INDEXLIB_TESTBASE
{
public:
    AttributeDocumentTest();
    ~AttributeDocumentTest();

    DECLARE_CLASS_NAME(AttributeDocumentTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeDocumentTest, TestSimpleProcess);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_ATTRIBUTEDOCUMENTTEST_H
