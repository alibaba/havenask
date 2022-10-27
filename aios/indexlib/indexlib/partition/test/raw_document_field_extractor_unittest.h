#ifndef __INDEXLIB_RAWDOCUMENTFIELDEXTRACTORTEST_H
#define __INDEXLIB_RAWDOCUMENTFIELDEXTRACTORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/raw_document_field_extractor.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitonSchema);
IE_NAMESPACE_BEGIN(partition);

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
    void TestSeekException();
    void TestValidateFields();

private:
    typedef RawDocumentFieldExtractor::FieldIterator FieldIterator;
private:
    void InnerCheckIterator(
        RawDocumentFieldExtractor& extractor,
        docid_t docId,
        const std::vector<std::string>& fieldNames,
        const std::string& fieldValues);
    
private:
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RawDocumentFieldExtractorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(RawDocumentFieldExtractorTest, TestExtractFromOfflinePartition);
INDEXLIB_UNIT_TEST_CASE(RawDocumentFieldExtractorTest, TestExtractFromSummary);
INDEXLIB_UNIT_TEST_CASE(RawDocumentFieldExtractorTest, TestSeekException);
INDEXLIB_UNIT_TEST_CASE(RawDocumentFieldExtractorTest, TestValidateFields);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_RAWDOCUMENTFIELDEXTRACTORTEST_H
