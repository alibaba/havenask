#pragma once

#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class IndexDocumentTest : public INDEXLIB_TESTBASE
{
public:
    IndexDocumentTest() {}
    ~IndexDocumentTest() {}

    DECLARE_CLASS_NAME(IndexDocumentTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForCreateField();
    void TestCaseForAddField();
    void TestCaseForSetField();
    void TestCaseForIterator();
    void TestCaseForNextInIterator();
    void TestCaseForHasNextInIterator();
    void TestCaseForOperatorEqual();
    void TestCaseForSetTermPayload();
    void TestCaseForSetDocPayload();
    void TestCaseSerializeIndexDocument();
    void TestCaseForSerializeSectionAttribute();
    void TestCaseForSerializeCompatibility();

private:
    void CreateDocument(IndexDocumentPtr& indexDoc, docid_t docId, int fieldCount, int sectionCount, int termCount);

    void CreateFields(const std::vector<fieldid_t>& fieldIds, std::vector<Field*>& fieldVec, uint32_t sectionsPerField,
                      uint32_t termCountPerSection);

private:
    autil::mem_pool::Pool* mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexDocumentTest, TestCaseForCreateField);
INDEXLIB_UNIT_TEST_CASE(IndexDocumentTest, TestCaseForSetField);
INDEXLIB_UNIT_TEST_CASE(IndexDocumentTest, TestCaseForAddField);
INDEXLIB_UNIT_TEST_CASE(IndexDocumentTest, TestCaseForIterator);
INDEXLIB_UNIT_TEST_CASE(IndexDocumentTest, TestCaseForNextInIterator);
INDEXLIB_UNIT_TEST_CASE(IndexDocumentTest, TestCaseForHasNextInIterator);
INDEXLIB_UNIT_TEST_CASE(IndexDocumentTest, TestCaseForOperatorEqual);
INDEXLIB_UNIT_TEST_CASE(IndexDocumentTest, TestCaseForSetTermPayload);
INDEXLIB_UNIT_TEST_CASE(IndexDocumentTest, TestCaseForSetDocPayload);
INDEXLIB_UNIT_TEST_CASE(IndexDocumentTest, TestCaseSerializeIndexDocument);
INDEXLIB_UNIT_TEST_CASE(IndexDocumentTest, TestCaseForSerializeSectionAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexDocumentTest, TestCaseForSerializeCompatibility);
}} // namespace indexlib::document
