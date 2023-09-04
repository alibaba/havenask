#ifndef __INDEXLIB_ATTRIBUTEDOCUMENTFIELDEXTRACTORTEST_H
#define __INDEXLIB_ATTRIBUTEDOCUMENTFIELDEXTRACTORTEST_H

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/attribute_document_field_extractor.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class AttributeDocumentFieldExtractorTest : public INDEXLIB_TESTBASE
{
public:
    AttributeDocumentFieldExtractorTest();
    ~AttributeDocumentFieldExtractorTest();

    DECLARE_CLASS_NAME(AttributeDocumentFieldExtractorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestExceptionCase();

private:
    void CheckValue(const AttributeDocumentFieldExtractorPtr& extractor, const config::AttributeSchemaPtr& attrSchema,
                    const document::AttributeDocumentPtr& attrDoc, fieldid_t fieldId, FieldType fieldType,
                    const std::vector<std::string>& expectValues);

private:
    autil::mem_pool::Pool mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeDocumentFieldExtractorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(AttributeDocumentFieldExtractorTest, TestExceptionCase);
}} // namespace indexlib::document

#endif //__INDEXLIB_ATTRIBUTEDOCUMENTFIELDEXTRACTORTEST_H
