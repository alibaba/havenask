#include <sstream>
#include <string>
#include <vector>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"
#include "indexlib/document/index_document/normal_document/test/document_serialize_test_helper.h"
#include "indexlib/test/unittest.h"

using namespace std;

#define NUM_DOCS 100

namespace indexlib { namespace document {

class SummaryDocumentTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SummaryDocumentTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForEqual()
    {
        vector<SummaryDocumentPtr> summaryDocuments1;
        vector<SummaryDocumentPtr> summaryDocuments2;
        GenerateSummaryDocuments(NUM_DOCS, summaryDocuments1);
        GenerateSummaryDocuments(NUM_DOCS, summaryDocuments2);
        for (size_t i = 0; i < summaryDocuments1.size(); ++i)
            for (size_t j = i; j < summaryDocuments2.size(); ++j) {
                if (i == j)
                    INDEXLIB_TEST_TRUE(*(summaryDocuments1[i]) == *(summaryDocuments2[j]));
                else
                    INDEXLIB_TEST_TRUE(*(summaryDocuments1[i]) != *(summaryDocuments2[j]));
            }
    }

    void TestCaseForIterator()
    {
        vector<SummaryDocumentPtr> documents;
        GenerateSummaryDocuments(10, documents);
        for (size_t i = 0; i < 10; ++i) {
            SummaryDocument::Iterator it = documents[i]->CreateIterator();
            for (size_t j = 0; j < i; ++j) {
                stringstream ss;
                ss << i << j;
                if (j % 2 == 0) {
                    INDEXLIB_TEST_TRUE(it.HasNext());
                    fieldid_t fieldId = 0;
                    const autil::StringView& value = it.Next(fieldId);
                    INDEXLIB_TEST_EQUAL((fieldid_t)j % 16, fieldId);
                    INDEXLIB_TEST_EQUAL(ss.str(), string(value.data(), value.size()));
                }
            }
            INDEXLIB_TEST_TRUE(!it.HasNext());
        }
    }

    void TestCaseSerializeSummaryDocument()
    {
        vector<SummaryDocumentPtr> summaryDocuments1;
        vector<SummaryDocumentPtr> summaryDocuments2;
        GenerateSummaryDocuments(NUM_DOCS, summaryDocuments1);

        autil::DataBuffer dataBuffer;
        autil::mem_pool::Pool pool;

        dataBuffer.write(summaryDocuments1);
        dataBuffer.read(summaryDocuments2, &pool);
        INDEXLIB_TEST_TRUE(summaryDocuments1.size() == summaryDocuments2.size());
        for (size_t i = 0; i < summaryDocuments1.size(); i++) {
            TEST_SUMMARY_DOCUMENT_SERIALIZE_EQUAL((*summaryDocuments1[i]), (*summaryDocuments2[i]));
        }
    }

    void TestCaseForSerializeWithFieldId()
    {
        autil::StringView expectValue = autil::MakeCString("abc", &mPool);
        fieldid_t expectFieldId = 100;

        SummaryDocumentPtr summaryDoc(new SummaryDocument);
        summaryDoc->SetField(expectFieldId, expectValue);

        autil::DataBuffer dataBuffer;
        dataBuffer.write(summaryDoc);

        // 1byte isNull
        // 1byte (storeFieldId = true), 1byte (notEmptyFieldCount),
        // 1byte (fieldId), 4 byte (value) = 7bytes
        ASSERT_EQ((int)8, dataBuffer.getDataLen());

        autil::mem_pool::Pool pool;
        SummaryDocumentPtr deSerSummaryDoc;
        dataBuffer.read(deSerSummaryDoc, &pool);
        TEST_SUMMARY_DOCUMENT_SERIALIZE_EQUAL((*summaryDoc), (*deSerSummaryDoc));

        SummaryDocument::Iterator iter = deSerSummaryDoc->CreateIterator();
        ASSERT_TRUE(iter.HasNext());

        fieldid_t fieldId = 0;
        const autil::StringView& value = iter.Next(fieldId);
        ASSERT_EQ(expectValue, value);
        ASSERT_EQ(expectFieldId, fieldId);

        ASSERT_FALSE(iter.HasNext());
    }

    void TestCaseForNeedPartialSerialize()
    {
        SummaryDocument summaryDoc;
        summaryDoc.SetField(0, autil::MakeCString("123", &mPool));

        // test all field is not empty
        ASSERT_FALSE(summaryDoc.NeedPartialSerialize());

        // test has empty field, no need store fieldId
        summaryDoc.SetField(2, autil::MakeCString("123", &mPool));
        ASSERT_FALSE(summaryDoc.NeedPartialSerialize());

        // test has empty field, need store fieldId
        summaryDoc.SetField(10, autil::MakeCString("123", &mPool));
        ASSERT_TRUE(summaryDoc.NeedPartialSerialize());
    }

    void TestCaseForSerializeAfterDeserialize()
    {
        InnerTestSerializeAfterDeserialize(true);
        InnerTestSerializeAfterDeserialize(false);

        // test empty doc
        SummaryDocumentPtr summaryDoc(new SummaryDocument);
        autil::DataBuffer dataBuffer;
        dataBuffer.write(summaryDoc);

        autil::mem_pool::Pool pool;
        SummaryDocumentPtr deSerSummaryDoc;
        dataBuffer.read(deSerSummaryDoc, &pool);
        TEST_SUMMARY_DOCUMENT_SERIALIZE_EQUAL((*summaryDoc), (*deSerSummaryDoc));
    }

    void InnerTestSerializeAfterDeserialize(bool useFieldId)
    {
        autil::StringView expectValue = autil::MakeCString("abc", &mPool);
        fieldid_t expectFieldId = useFieldId ? 100 : 1;

        SummaryDocumentPtr summaryDoc(new SummaryDocument);
        summaryDoc->SetField(expectFieldId, expectValue);

        autil::DataBuffer dataBuffer;
        dataBuffer.write(summaryDoc);

        autil::mem_pool::Pool pool;
        SummaryDocumentPtr deSerSummaryDoc;
        dataBuffer.read(deSerSummaryDoc, &pool);
        TEST_SUMMARY_DOCUMENT_SERIALIZE_EQUAL((*summaryDoc), (*deSerSummaryDoc));

        autil::DataBuffer dataBuffer2;
        dataBuffer2.write(deSerSummaryDoc);

        SummaryDocumentPtr reSerSummaryDoc;
        dataBuffer2.read(reSerSummaryDoc, &pool);
        TEST_SUMMARY_DOCUMENT_SERIALIZE_EQUAL((*summaryDoc), (*reSerSummaryDoc));
    }

private:
    void GenerateSummaryDocuments(size_t num, vector<SummaryDocumentPtr>& documents)
    {
        for (size_t i = 0; i < num; ++i) {
            SummaryDocumentPtr ptr(new SummaryDocument);
            ptr->SetDocId(i);
            for (size_t j = 0; j < i; ++j) {
                stringstream ss;
                ss << i << j;
                if (j % 2 == 0) {
                    ptr->SetField(j % 16, autil::MakeCString(ss.str(), &mPool));
                }
            }
            documents.push_back(ptr);
        }
    }

private:
    autil::mem_pool::Pool mPool;
};

INDEXLIB_UNIT_TEST_CASE(SummaryDocumentTest, TestCaseForEqual);
INDEXLIB_UNIT_TEST_CASE(SummaryDocumentTest, TestCaseForIterator);
INDEXLIB_UNIT_TEST_CASE(SummaryDocumentTest, TestCaseSerializeSummaryDocument);
INDEXLIB_UNIT_TEST_CASE(SummaryDocumentTest, TestCaseForSerializeWithFieldId);
INDEXLIB_UNIT_TEST_CASE(SummaryDocumentTest, TestCaseForNeedPartialSerialize);
INDEXLIB_UNIT_TEST_CASE(SummaryDocumentTest, TestCaseForSerializeAfterDeserialize);
}} // namespace indexlib::document
