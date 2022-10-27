#include "indexlib/document/index_document/normal_document/test/serialized_summary_document_unittest.h"
#include "indexlib/document/index_document/normal_document/test/document_serialize_test_helper.h"

using namespace std;

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, SerializedSummaryDocumentTest);

SerializedSummaryDocumentTest::SerializedSummaryDocumentTest()
{
}

SerializedSummaryDocumentTest::~SerializedSummaryDocumentTest()
{
}

void SerializedSummaryDocumentTest::CaseSetUp()
{
}

void SerializedSummaryDocumentTest::CaseTearDown()
{
}

void SerializedSummaryDocumentTest::TestSerialize()
{
    {
        SerializedSummaryDocument doc1, doc2;
        char *str = new char[10];
        memcpy(str, "1234512345", 5);
        doc1.SetSerializedDoc(str , 5);
        autil::DataBuffer dataBuffer;
        dataBuffer.write(doc1);
        dataBuffer.read(doc2);
        TEST_SUMMARY_SERIALIZED_DOCUMENT_SERIALIZE_EQUAL(doc1, doc2);
    }
    {
        SerializedSummaryDocument doc1, doc2;
        doc1.SetSerializedDoc(new char[0], 0);
        autil::DataBuffer dataBuffer;
        dataBuffer.write(doc1);
        dataBuffer.read(doc2);
        TEST_SUMMARY_SERIALIZED_DOCUMENT_SERIALIZE_EQUAL(doc1, doc2);
    }

}

IE_NAMESPACE_END(document);

