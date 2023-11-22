#pragma once

#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

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
    void TestNullField();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeDocumentTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(AttributeDocumentTest, TestNullField);
}} // namespace indexlib::document
