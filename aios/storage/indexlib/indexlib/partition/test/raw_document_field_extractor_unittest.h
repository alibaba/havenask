#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/raw_document_field_extractor.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitonSchema);
namespace indexlib { namespace partition {

class RawDocumentFieldExtractorTest : public INDEXLIB_TESTBASE
{
public:
    RawDocumentFieldExtractorTest();
    ~RawDocumentFieldExtractorTest();

    DECLARE_CLASS_NAME(RawDocumentFieldExtractorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestExtractFromOfflinePartition();
    void TestExtractFromSummary();
    void TestExtractFromSourceIndex();
    void TestValidateFields();
    void TestExtractNullField();
    void TestExtractTimeAttribute();

private:
    typedef RawDocumentFieldExtractor::FieldIterator FieldIterator;

private:
    void InnerCheckIterator(RawDocumentFieldExtractor& extractor, docid_t docId,
                            const std::vector<std::string>& fieldNames, const std::string& fieldValues);

private:
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RawDocumentFieldExtractorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(RawDocumentFieldExtractorTest, TestExtractFromOfflinePartition);
INDEXLIB_UNIT_TEST_CASE(RawDocumentFieldExtractorTest, TestExtractFromSummary);
INDEXLIB_UNIT_TEST_CASE(RawDocumentFieldExtractorTest, TestValidateFields);
INDEXLIB_UNIT_TEST_CASE(RawDocumentFieldExtractorTest, TestExtractNullField);
INDEXLIB_UNIT_TEST_CASE(RawDocumentFieldExtractorTest, TestExtractTimeAttribute);
INDEXLIB_UNIT_TEST_CASE(RawDocumentFieldExtractorTest, TestExtractFromSourceIndex);
}} // namespace indexlib::partition
