#include "build_service/document/ExtendDocument.h"

#include "autil/StringUtil.h"
#include "build_service/test/unittest.h"
#include "indexlib/document/raw_document/default_raw_document.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::document;

namespace build_service { namespace document {

class ExtendDocumentTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

protected:
    ExtendDocumentPtr createExtendDocument(int64_t timestamp, const std::string& modifiedFields,
                                           const std::string& subModifiedFields = "");

    // void checkIndexDocument(const indexlib::document::DocumentPtr& indexDoc,
    //                         int64_t timestamp, const std::string modifiedFields,
    //                         const std::string subModifiedFields, size_t subDocCount);
};

void ExtendDocumentTest::setUp() {}

void ExtendDocumentTest::tearDown() {}

TEST_F(ExtendDocumentTest, testSimpleProcess)
{
    BS_LOG(DEBUG, "Begin Test!");
    {
        ExtendDocument document;
        ASSERT_EQ((size_t)0, document.getFieldAnalyzerNameMap().size());
        fieldid_t id1 = 1;
        document.setFieldAnalyzerName(id1, "extendAnalyzer");
        ASSERT_EQ(string("extendAnalyzer"), document.getFieldAnalyzerName(id1));
        ASSERT_EQ((size_t)1, document.getFieldAnalyzerNameMap().size());
    }
    {
        ExtendDocument document;
        ASSERT_EQ((size_t)0, document.getFieldAnalyzerNameMap().size());
        ExtendDocument::FieldAnalyzerNameMap fieldAnalyzerNameMap;
        fieldid_t id1 = 1;
        fieldAnalyzerNameMap[id1] = "extendAnalyzer";
        document.setFieldAnalyzerNameMap(fieldAnalyzerNameMap);
        ASSERT_EQ(string("extendAnalyzer"), document.getFieldAnalyzerName(id1));
        ASSERT_EQ((size_t)1, document.getFieldAnalyzerNameMap().size());
    }
}

ExtendDocumentPtr ExtendDocumentTest::createExtendDocument(int64_t timestamp, const string& modifiedFields,
                                                           const string& subModifiedFields)
{
    ExtendDocumentPtr doc(new ExtendDocument);
    RawDocumentHashMapManagerPtr hashMapManager(new RawDocumentHashMapManager());
    RawDocumentPtr rawDocPtr(new DefaultRawDocument(hashMapManager));
    doc->setRawDocument(rawDocPtr);

    if (timestamp != -1) {
        rawDocPtr->setDocTimestamp(timestamp);
    }
    if (!modifiedFields.empty()) {
        vector<fieldid_t> inputModifiedFields;
        StringUtil::fromString(modifiedFields, inputModifiedFields, ";");
        set<fieldid_t> destModifiedFields(inputModifiedFields.begin(), inputModifiedFields.end());
        doc->getLegacyExtendDoc()->setModifiedFieldSet(destModifiedFields);
    }

    if (!subModifiedFields.empty()) {
        vector<fieldid_t> inputModifiedFields;
        StringUtil::fromString(subModifiedFields, inputModifiedFields, ";");
        set<fieldid_t> destModifiedFields(inputModifiedFields.begin(), inputModifiedFields.end());
        doc->getLegacyExtendDoc()->setSubModifiedFieldSet(destModifiedFields);
    }

    return doc;
}

// void ExtendDocumentTest::checkIndexDocument(const DocumentPtr& indexDoc,
//         int64_t timestamp, const std::string modifiedFields,
//         const std::string subModifiedFields, size_t subDocCount)
// {
//     ASSERT_TRUE(indexDoc);
//     ASSERT_EQ(timestamp, indexDoc->GetTimestamp());
//     ASSERT_EQ(subDocCount, indexDoc->GetSubDocuments().size());

//     vector<fieldid_t> expectModifiedFields;
//     StringUtil::fromString(modifiedFields, expectModifiedFields, ";");
//     const vector<fieldid_t> & actualModifiedFields = indexDoc->GetModifiedFields();
//     ASSERT_EQ(expectModifiedFields.size(), actualModifiedFields.size());
//     for (size_t i = 0; i < expectModifiedFields.size(); ++i) {
//         ASSERT_EQ(expectModifiedFields[i], actualModifiedFields[i]);
//     }

//     vector<fieldid_t> expectSubModifiedFields;
//     StringUtil::fromString(subModifiedFields, expectSubModifiedFields, ";");
//     const vector<fieldid_t> & actualSubModifiedFields =
//         indexDoc->GetSubModifiedFields();
//     ASSERT_EQ(expectSubModifiedFields.size(), actualSubModifiedFields.size());
//     for (size_t i = 0; i < expectSubModifiedFields.size(); ++i) {
//         ASSERT_EQ(expectSubModifiedFields[i], actualSubModifiedFields[i]);
//     }
// }

// TEST_F(ExtendDocumentTest, testCreateDocument) {
//     {
//         ExtendDocumentPtr doc = createExtendDocument(123456789, "1");
//         DocumentPtr indexDoc(doc->createDocument());
//         checkIndexDocument(indexDoc, 123456789, "1", "", 0);
//     }
//     {
//         ExtendDocumentPtr doc = createExtendDocument(123456789, "");
//         doc->getRawDocument()->setDocOperateType(UPDATE_FIELD);
//         ExtendDocumentPtr subDoc1 = createExtendDocument(-1, "");
//         doc->addSubDocument(subDoc1);
//         DocumentPtr indexDoc(doc->createDocument());
//         checkIndexDocument(indexDoc, 123456789, "", "", 1);
//         const vector<DocumentPtr> &subIndexDocs = indexDoc->GetSubDocuments();
//         checkIndexDocument(subIndexDocs[0], 123456789, "", "", 0);
//         ASSERT_EQ(UPDATE_FIELD, subIndexDocs[0]->GetDocOperateType());
//     }
//     {
//         ExtendDocumentPtr doc = createExtendDocument(123456789, "1");
//         // sub doc use main doc's timestamp
//         ExtendDocumentPtr subDoc1 = createExtendDocument(-1, "2");
//         doc->addSubDocument(subDoc1);
//         ExtendDocumentPtr subDoc2 = createExtendDocument(-1, "2");
//         doc->addSubDocument(subDoc2);

//         DocumentPtr indexDoc(doc->createDocument());
//         checkIndexDocument(indexDoc, 123456789, "1", "", 2);

//         const vector<DocumentPtr> &subIndexDocs = indexDoc->GetSubDocuments();
//         checkIndexDocument(subIndexDocs[0], 123456789, "2", "", 0);
//         checkIndexDocument(subIndexDocs[1], 123456789, "2", "", 0);
//     }
// }

// TEST_F(ExtendDocumentTest, testCreateDocumentForSub) {
//     ExtendDocumentPtr doc = createExtendDocument(123456789, "1", "2");
//     ExtendDocumentPtr subDoc1 = createExtendDocument(-1, "2");
//     doc->addSubDocument(subDoc1);
//     ExtendDocumentPtr subDoc2 = createExtendDocument(-1, "");
//     doc->addSubDocument(subDoc2);

//     DocumentPtr indexDoc(doc->createDocument());
//     checkIndexDocument(indexDoc, 123456789, "1", "2", 2);
//     const vector<DocumentPtr> &subIndexDocs = indexDoc->GetSubDocuments();
//     checkIndexDocument(subIndexDocs[0], 123456789, "2", "", 0);
//     checkIndexDocument(subIndexDocs[1], 123456789, "", "", 0);
// }

}} // namespace build_service::document
