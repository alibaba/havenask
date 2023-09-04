#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil::mem_pool;

namespace indexlib { namespace document {

class SearchSummaryDocumentTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SearchSummaryDocumentTest);
    void CaseSetUp() override { pool = new Pool(4096); }

    void CaseTearDown() override { delete pool; }

    void TestCaseForGetBuffer()
    {
        SearchSummaryDocument* searchSummaryDoc = new SearchSummaryDocument(pool, 2);
        char* value = searchSummaryDoc->GetBuffer(32);
        char* value1 = searchSummaryDoc->GetBuffer(32);
        char* value2 = searchSummaryDoc->GetBuffer(32);
        INDEXLIB_TEST_TRUE(value1 - value == value2 - value1);
        delete searchSummaryDoc;
    }

    void TestCaseForAddConstString()
    {
        size_t size = 2048;
        string testData = "aaaaaaaaaaaa";
        SearchSummaryDocument* searchSummaryDoc = new SearchSummaryDocument(pool, size);
        INDEXLIB_TEST_EQUAL(size, searchSummaryDoc->_valPool.size());
        for (size_t i = 0; i < size; i++) {
            INDEXLIB_TEST_EQUAL(nullptr, searchSummaryDoc->_valPool[i]->data());
            INDEXLIB_TEST_EQUAL(0, searchSummaryDoc->_valPool[i]->size());
        }
        searchSummaryDoc->addConstString(10);
        INDEXLIB_TEST_EQUAL(size, searchSummaryDoc->_valPool.size());
        searchSummaryDoc->addConstString(size * 2);
        INDEXLIB_TEST_EQUAL(2 * size, searchSummaryDoc->_valPool.size());
        for (size_t i = 0; i < 2 * size; i++) {
            INDEXLIB_TEST_EQUAL(nullptr, searchSummaryDoc->_valPool[i]->data());
            INDEXLIB_TEST_EQUAL(0, searchSummaryDoc->_valPool[i]->size());
            *searchSummaryDoc->_valPool[i] = {testData.c_str(), testData.size()};
        }
        for (size_t i = 0; i < 2 * size; i++) {
            INDEXLIB_TEST_EQUAL(testData, searchSummaryDoc->_valPool[i]->to_string());
        }

        delete searchSummaryDoc;
    }

    void TestCaseForAllocateFromBuffer()
    {
        SearchSummaryDocument searchSummaryDoc = SearchSummaryDocument(pool, 2);
        char* value1 = searchSummaryDoc.AllocateFromBuffer(8);
        INDEXLIB_TEST_TRUE(NULL != value1);
        char* value2 = searchSummaryDoc.AllocateFromBuffer(2048);
        INDEXLIB_TEST_TRUE(value1 != value2);
        INDEXLIB_TEST_TRUE(NULL != value2);
        char* value3 = searchSummaryDoc.AllocateFromBuffer(8);
        INDEXLIB_TEST_TRUE(value1 != value3);
        INDEXLIB_TEST_TRUE(value2 != value3);
        INDEXLIB_TEST_TRUE(NULL != value3);
        char* value4 = searchSummaryDoc.AllocateFromBuffer(2048);
        INDEXLIB_TEST_EQUAL(NULL, value4);
    }

    void TestCaseForUpdateFieldCount()
    {
        SearchSummaryDocument searchSummaryDoc = SearchSummaryDocument(pool, 2);
        const char* value0 = "value0";
        const char* value1 = "value1";
        INDEXLIB_TEST_TRUE(searchSummaryDoc.SetFieldValue(0, value0, strlen(value0), false));
        INDEXLIB_TEST_TRUE(value0 == searchSummaryDoc.GetFieldValue(0)->data());
        INDEXLIB_TEST_TRUE(searchSummaryDoc.SetFieldValue(1, value1, strlen(value1), false));
        INDEXLIB_TEST_TRUE(value1 == searchSummaryDoc.GetFieldValue(1)->data());
        searchSummaryDoc.UpdateFieldCount(1);
        INDEXLIB_TEST_TRUE(value0 == searchSummaryDoc.GetFieldValue(0)->data());
        INDEXLIB_TEST_TRUE(value1 == searchSummaryDoc.GetFieldValue(1)->data());
    }

    void TestCaseForSetFieldValue()
    {
        SearchSummaryDocument* searchSummaryDoc = new SearchSummaryDocument(pool, 2);
        const char* value = "TestCaseForSetFieldValue";
        INDEXLIB_TEST_TRUE(searchSummaryDoc->SetFieldValue(0, value, strlen(value), false));
        INDEXLIB_TEST_TRUE(value == searchSummaryDoc->GetFieldValue(0)->data());

        INDEXLIB_TEST_TRUE(searchSummaryDoc->SetFieldValue(1, value, strlen(value)));
        INDEXLIB_TEST_TRUE(value != searchSummaryDoc->GetFieldValue(1)->data());

        INDEXLIB_TEST_TRUE(searchSummaryDoc->SetFieldValue(2, value, strlen(value)));
        INDEXLIB_TEST_EQUAL((size_t)3, searchSummaryDoc->GetFieldCount());

        searchSummaryDoc->ClearFieldValue(1);
        INDEXLIB_TEST_TRUE(!searchSummaryDoc->GetFieldValue(1));

        delete searchSummaryDoc;
    }

    void TestCaseForGetFieldValue()
    {
        SearchSummaryDocument* searchSummaryDoc = new SearchSummaryDocument(pool, 2);
        const char* value = "TestCaseForGetFieldValue";
        searchSummaryDoc->SetFieldValue(1, value, strlen(value));
        int a = memcmp(searchSummaryDoc->GetFieldValue(1)->data(), value, strlen(value));
        INDEXLIB_TEST_TRUE(0 == a);
        delete searchSummaryDoc;
    }

private:
    Pool* pool;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(document, SearchSummaryDocumentTest);

INDEXLIB_UNIT_TEST_CASE(SearchSummaryDocumentTest, TestCaseForGetBuffer);
INDEXLIB_UNIT_TEST_CASE(SearchSummaryDocumentTest, TestCaseForSetFieldValue);
INDEXLIB_UNIT_TEST_CASE(SearchSummaryDocumentTest, TestCaseForGetFieldValue);
INDEXLIB_UNIT_TEST_CASE(SearchSummaryDocumentTest, TestCaseForAddConstString);
}} // namespace indexlib::document
