#include "build_service/test/unittest.h"
#include "build_service/reader/StandardRawDocumentParser.h"
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace build_service::document;
namespace build_service {
namespace reader {

class StandardRawDocumentParserTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    void testParseForDocFormat(const std::string &fieldSep,
                               const std::string &keyValueSep);
private:
    std::string generateDocString(const std::string &fieldSep,
                                  const std::string &keyValueSep,
                                  const KeyValueMap &kvMap);
    void testParse(
            const std::string &fieldSep, const std::string &keyValueSep,
            const std::string &docStr, bool expectedParseResult,
            const KeyValueMap &expectedRawDoc = KeyValueMap());
protected:
    std::string _fieldSep;
    std::string _keyValueSep;
private:
    RawDocumentHashMapManagerPtr _hashMapManager;
};

void StandardRawDocumentParserTest::setUp() {
    _hashMapManager.reset(new RawDocumentHashMapManager());
}

void StandardRawDocumentParserTest::tearDown() {
}

void StandardRawDocumentParserTest::testParseForDocFormat(
        const string &fieldSep, const string &keyValueSep)
{
    // empty string
    testParse(fieldSep, keyValueSep, "", false);
    // invalid format
    KeyValueMap kvMap;
    kvMap["fieldName"] = "fieldValue";
    if (keyValueSep != "=") {
        testParse(fieldSep, keyValueSep, "fieldName=fieldValue", false);
    } else {
        testParse(fieldSep, keyValueSep, "fieldName=fieldValue", true, kvMap);
    }
    // one field
    // KeyValueMap kvMap;
    kvMap.clear();
    kvMap["fieldName"] = "fieldValue";
    testParse(fieldSep, keyValueSep, generateDocString(fieldSep, keyValueSep, kvMap),
              true, kvMap);
    // many fields
    kvMap.clear();
    kvMap["a1"] = "";
    kvMap["f1"] = "v1";
    kvMap["f2"] = "v2";
    kvMap["f3"] = "v3";
    kvMap["f4"] = "v4";
    kvMap["f5"] = "v5";
    kvMap["f6"] = "";
    testParse(fieldSep, keyValueSep, generateDocString(fieldSep, keyValueSep, kvMap),
              true, kvMap);
}

string StandardRawDocumentParserTest::generateDocString(
        const string &fieldSep, const string &keyValueSep,
        const KeyValueMap &kvMap)
{
    string docStr;
    size_t i = 0;
    for (KeyValueMap::const_iterator it = kvMap.begin();
         it != kvMap.end(), i < kvMap.size(); it++, i++)
    {
        bool insertEmpty = rand() % 2 == 0;
        if (insertEmpty) {
            docStr += ' ';
        }
        docStr += it->first;
        if (insertEmpty) {
            docStr += '\n';
        }
        docStr += keyValueSep;
        docStr += it->second;
        if (i+1 != kvMap.size()) {
            docStr += fieldSep;
        }
    }
    return docStr;
}

void StandardRawDocumentParserTest::testParse(
        const string &fieldSep, const string &keyValueSep,
        const string &docStr, bool expectedParseResult,
        const KeyValueMap &expectedRawDoc)
{
    cout << "docStr:" << docStr << endl;
    StandardRawDocumentParser parser(fieldSep, keyValueSep);
    IE_NAMESPACE(document)::DefaultRawDocument rawDoc(_hashMapManager);
    bool ret = parser.parse(docStr, rawDoc);
    ASSERT_EQ(expectedParseResult, ret);
    if (ret) {
        ASSERT_EQ((uint32_t)expectedRawDoc.size() + 1, rawDoc.getFieldCount());
        for (KeyValueMap::const_iterator it = expectedRawDoc.begin();
             it != expectedRawDoc.end(); it++)
        {
            ASSERT_EQ(it->second, rawDoc.getField(it->first));
        }
    }

    if (expectedRawDoc.find("a1") != expectedRawDoc.end()) {
        ASSERT_TRUE(rawDoc.exist("a1"));
    }

    if (expectedRawDoc.find("f6") != expectedRawDoc.end()) {
        ASSERT_TRUE(rawDoc.exist("f6"));
    }
}

TEST_F(StandardRawDocumentParserTest, testParseHa3Doc) {
    testParseForDocFormat("\x1F\n", "=");
}

TEST_F(StandardRawDocumentParserTest, testParseISearchDoc) {
    testParseForDocFormat("\x01\n", "=");
}

TEST_F(StandardRawDocumentParserTest, testParseOtherFormat) {
    testParseForDocFormat("aaaaa", "bbbbb");
}

TEST_F(StandardRawDocumentParserTest, testParseWithSameSep) {
    testParseForDocFormat("aa", "aa");
}

TEST_F(StandardRawDocumentParserTest, testParseWithSepInFieldNameOrFieldValue) {
    StandardRawDocumentParser parser("\x1F\n", "=");
    IE_NAMESPACE(document)::DefaultRawDocument rawDoc(_hashMapManager);
    string docString("fieldName==fieldValue=\x1F\n");
    ASSERT_TRUE(parser.parse(docString, rawDoc));
    ASSERT_EQ(size_t(2), rawDoc.getFieldCount());
    ASSERT_EQ("=fieldValue=", rawDoc.getField("fieldName"));
    ASSERT_EQ(CMD_ADD, rawDoc.getField(CMD_TAG));
}

TEST_F(StandardRawDocumentParserTest, testParseWithFieldNameEmpty) {
    StandardRawDocumentParser parser("\x1F\n", "=");
    IE_NAMESPACE(document)::DefaultRawDocument rawDoc(_hashMapManager);
    string docString("=fieldValue\x1F\n");
    ASSERT_FALSE(parser.parse(docString, rawDoc));
}

TEST_F(StandardRawDocumentParserTest, testCmdExist) {
    StandardRawDocumentParser parser("\x1F\n", "=");
    IE_NAMESPACE(document)::DefaultRawDocument rawDoc(_hashMapManager);
    string docString("CMD=update_field\x1F\n");
    ASSERT_TRUE(parser.parse(docString, rawDoc));
    ASSERT_EQ(size_t(1), rawDoc.getFieldCount());
    ASSERT_EQ(CMD_UPDATE_FIELD, rawDoc.getField(CMD_TAG));
}

TEST_F(StandardRawDocumentParserTest, testMultiFieldsWithHa3Doc) {
    StandardRawDocumentParser parser("\x1F\n", "=");
    IE_NAMESPACE(document)::DefaultRawDocument rawDoc(_hashMapManager);
    string docString("CMD=delete\x1F\nfieldName=fieldValue\x1F\nfieldName1=fieldValue1\x1F\n");
    ASSERT_TRUE(parser.parse(docString, rawDoc));
    ASSERT_EQ(size_t(3), rawDoc.getFieldCount());
    ASSERT_EQ("fieldValue", rawDoc.getField("fieldName"));
    ASSERT_EQ("fieldValue1", rawDoc.getField("fieldName1"));
    ASSERT_EQ(CMD_DELETE, rawDoc.getField(CMD_TAG));
}

TEST_F(StandardRawDocumentParserTest, testCmdDeleteSub) {
    StandardRawDocumentParser parser("\x1F\n", "=");
    IE_NAMESPACE(document)::DefaultRawDocument rawDoc(_hashMapManager);
    string docString("CMD=delete_sub\x1F\n");
    ASSERT_TRUE(parser.parse(docString, rawDoc));
    ASSERT_EQ(size_t(1), rawDoc.getFieldCount());
    ASSERT_EQ(CMD_DELETE_SUB, rawDoc.getField(CMD_TAG));
}

TEST_F(StandardRawDocumentParserTest, testLastFieldNoSeperator) {
    StandardRawDocumentParser parser("&", "=");
    string docString("a=b&c=d");
    IE_NAMESPACE(document)::DefaultRawDocument rawDoc(_hashMapManager);
    ASSERT_TRUE(parser.parse(docString, rawDoc));
    ASSERT_EQ(size_t(3), rawDoc.getFieldCount());
    ASSERT_EQ("b", rawDoc.getField("a"));
    ASSERT_EQ("d", rawDoc.getField("c"));
}

}
}
