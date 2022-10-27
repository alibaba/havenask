#include "build_service/processor/SubDocumentExtractor.h"
#include "build_service/test/unittest.h"
#include <indexlib/config/index_partition_schema_maker.h>
#include <autil/StringUtil.h>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
using namespace build_service::document;

namespace build_service {
namespace processor {

class SubDocumentExtractorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    void DoTestExtractSubDocForAbnormalSubFieldValue(
            const std::string& orgFieldValue,
            const std::string& expectFieldValue);
    void DoTestExtractSubDocForAbnormalSubFieldLen(
        const std::string& orgField1Value,
        const std::string& orgField2Value,
        const std::string& expectField1Value,
        const std::string& expectField2Value);

protected:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schema;
};

void SubDocumentExtractorTest::setUp() {
    _schema.reset(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(_schema,
            "title:string;", //Field schema
            "",//index schema
            "",//attribute schema
            "");//Summary schema
    IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(subSchema,
            "sub_title:text;single_int_field:int8:false;"
            "multi_int_field:int8:true;string_field:string;",
            "", "", "");
    _schema->SetSubIndexPartitionSchema(subSchema);
}

void SubDocumentExtractorTest::tearDown() {
}

TEST_F(SubDocumentExtractorTest, testSimpleProcess) {
    BS_LOG(DEBUG, "Begin Test!");

    ASSERT_EQ(document::SUB_DOC_SEPARATOR, '');

    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField("title", "main title");
    rawDoc->setField("sub_title", "firstsecond");

    SubDocumentExtractor extractor(_schema);
    vector<RawDocumentPtr> subDocs;

    extractor.extractSubDocuments(rawDoc, subDocs);
    ASSERT_EQ(string("main title"), rawDoc->getField("title"));
    ASSERT_EQ(string("firstsecond"), rawDoc->getField("sub_title"));

    ASSERT_EQ((size_t)2, subDocs.size());
    ASSERT_EQ(string("first"), subDocs[0]->getField("sub_title"));
    ASSERT_EQ(string("second"), subDocs[1]->getField("sub_title"));
}

TEST_F(SubDocumentExtractorTest, testExtractSubDoc) {
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField("title", "main title");
    rawDoc->setField("sub_title", "firstsecond");
    rawDoc->setField("single_int_field", "12");
    rawDoc->setField("multi_int_field", "12345");
    rawDoc->setField("string_field", "abcdef");

    SubDocumentExtractor extractor(_schema);
    vector<RawDocumentPtr> subDocs;

    extractor.extractSubDocuments(rawDoc, subDocs);
    ASSERT_EQ(string("main title"), rawDoc->getField("title"));
    ASSERT_EQ(string("firstsecond"), rawDoc->getField("sub_title"));
    ASSERT_EQ(string("12"), rawDoc->getField("single_int_field"));
    ASSERT_EQ(string("12345"), rawDoc->getField("multi_int_field"));
    ASSERT_EQ(string("abcdef"), rawDoc->getField("string_field"));

    ASSERT_EQ((size_t)2, subDocs.size());
    ASSERT_EQ(string("first"), subDocs[0]->getField("sub_title"));
    ASSERT_EQ(string("second"), subDocs[1]->getField("sub_title"));
    ASSERT_EQ(string("1"), subDocs[0]->getField("single_int_field"));
    ASSERT_EQ(string("2"), subDocs[1]->getField("single_int_field"));
    ASSERT_EQ(string("12"), subDocs[0]->getField("multi_int_field"));
    ASSERT_EQ(string("345"), subDocs[1]->getField("multi_int_field"));
    ASSERT_EQ(string("abc"), subDocs[0]->getField("string_field"));
    ASSERT_EQ(string("def"), subDocs[1]->getField("string_field"));
}

TEST_F(SubDocumentExtractorTest, testExtractSubDocForNoSubDoc) {
    {
        RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
        rawDoc->setField("title", "main title");

        SubDocumentExtractor extractor(_schema);
        vector<RawDocumentPtr> subDocs;

        extractor.extractSubDocuments(rawDoc, subDocs);
        ASSERT_EQ((size_t)0, subDocs.size());
    }
    {
        RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
        rawDoc->setField("title", "main title");
        rawDoc->setField("sub_title", "");

        SubDocumentExtractor extractor(_schema);
        vector<RawDocumentPtr> subDocs;

        extractor.extractSubDocuments(rawDoc, subDocs);
        ASSERT_EQ((size_t)0, subDocs.size());
    }
}

void SubDocumentExtractorTest::DoTestExtractSubDocForAbnormalSubFieldValue(
        const string& orgFieldValue, const string& expectFieldValue)
{
    vector<string> expectFieldValues = StringUtil::split(expectFieldValue, ",");
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField("title", "main title");
    rawDoc->setField("sub_title", orgFieldValue);
    SubDocumentExtractor extractor(_schema);
    vector<RawDocumentPtr> subDocs;
    extractor.extractSubDocuments(rawDoc, subDocs);
    ASSERT_EQ(expectFieldValues.size(), subDocs.size());
    for (size_t i = 0; i < expectFieldValues.size(); ++i) {
        if (expectFieldValues[i] == "__NULL__") {
            ASSERT_EQ(string(""), subDocs[i]->getField("sub_title"));
        }
        else {
            ASSERT_EQ(expectFieldValues[i],
                             subDocs[i]->getField("sub_title"));
        }
    }
}

TEST_F(SubDocumentExtractorTest, testExtractSubDocForAbnormalSubFieldValue) {
    DoTestExtractSubDocForAbnormalSubFieldValue("firstthird",
            "first,__NULL__,third");
    DoTestExtractSubDocForAbnormalSubFieldValue("firstsecond",
            "first,second,__NULL__");
    DoTestExtractSubDocForAbnormalSubFieldValue("secondthird",
            "__NULL__,second,third");
    DoTestExtractSubDocForAbnormalSubFieldValue("",
            "__NULL__,__NULL__");
}

void SubDocumentExtractorTest::DoTestExtractSubDocForAbnormalSubFieldLen(
        const string& orgField1Value, const string& orgField2Value,
        const string& expectField1Value, const string& expectField2Value)
{
    vector<string> expectField1Values = StringUtil::split(expectField1Value, ",");
    vector<string> expectField2Values = StringUtil::split(expectField2Value, ",");
    assert(expectField1Values.size() == expectField2Values.size());

    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField("title", "main title");
    rawDoc->setField("sub_title", orgField1Value);
    rawDoc->setField("single_int_field", orgField2Value);
    SubDocumentExtractor extractor(_schema);
    vector<RawDocumentPtr> subDocs;
    extractor.extractSubDocuments(rawDoc, subDocs);
    ASSERT_EQ(expectField1Values.size(), subDocs.size());
    for (size_t i = 0; i < expectField1Values.size(); ++i) {
        if (expectField1Values[i] == "__NULL__") {
            ASSERT_EQ(string(""), subDocs[i]->getField("sub_title"));
        }
        else {
            ASSERT_EQ(expectField1Values[i],
                    subDocs[i]->getField("sub_title"));
        }
        if (expectField2Values[i] == "__NULL__") {
            ASSERT_EQ(string(""),
                    subDocs[i]->getField("single_int_field"));
        }
        else {
            ASSERT_EQ(expectField2Values[i],
                    subDocs[i]->getField("single_int_field"));
        }
    }
}

TEST_F(SubDocumentExtractorTest, testExtractSubDocForAbnormalSubFieldLen) {
    DoTestExtractSubDocForAbnormalSubFieldLen("", "123456",
            "__NULL__,__NULL__", "123,456");
    DoTestExtractSubDocForAbnormalSubFieldLen("123456", "",
            "123,456", "__NULL__,__NULL__");
    DoTestExtractSubDocForAbnormalSubFieldLen("abc", "123456",
            "abc,__NULL__", "123,456");
    DoTestExtractSubDocForAbnormalSubFieldLen("123456", "abc",
            "123,456", "abc,__NULL__");
}

TEST_F(SubDocumentExtractorTest, testExtractSubDocWithReserveFields) {
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField(CMD_TAG, "ADD");
    rawDoc->setField(HA_RESERVED_TIMESTAMP, "123456");
    rawDoc->setField(HA_RESERVED_MODIFY_FIELDS, "field_1;field_2");
    rawDoc->setField("sub_title", "firstsecond");

    SubDocumentExtractor extractor(_schema);
    RawDocumentPtr doc(new IE_NAMESPACE(document)::DefaultRawDocument());
    vector<RawDocumentPtr> subDocs;
    extractor.extractSubDocuments(rawDoc, subDocs);
    ASSERT_EQ((size_t)2, subDocs.size());

    for (size_t i = 0; i < subDocs.size(); ++i)
    {
        ASSERT_EQ(string("ADD"), subDocs[i]->getField(CMD_TAG));
        ASSERT_EQ(string("123456"),
                             subDocs[i]->getField(HA_RESERVED_TIMESTAMP));
        ASSERT_EQ(string(""), subDocs[i]->getField(HA_RESERVED_MODIFY_FIELDS));
    }
    ASSERT_EQ(string("ADD"), rawDoc->getField(CMD_TAG));
    ASSERT_EQ(string("123456"),
                         rawDoc->getField(HA_RESERVED_TIMESTAMP));
    ASSERT_EQ(string("field_1;field_2"),
                         rawDoc->getField(HA_RESERVED_MODIFY_FIELDS));
}

TEST_F(SubDocumentExtractorTest, testExtractSubDocWithSubModifier) {
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField(CMD_TAG, "ADD");
    rawDoc->setField(HA_RESERVED_MODIFY_FIELDS, "field_1;field_2;sub_1;sub_2");
    rawDoc->setField(HA_RESERVED_SUB_MODIFY_FIELDS, "sub_1;sub_2sub_2");
    rawDoc->setField("sub_title", "firstsecond");

    SubDocumentExtractor extractor(_schema);
    RawDocumentPtr doc(new IE_NAMESPACE(document)::DefaultRawDocument());
    vector<RawDocumentPtr> subDocs;
    extractor.extractSubDocuments(rawDoc, subDocs);
    ASSERT_EQ((size_t)2, subDocs.size());

    for (size_t i = 0; i < subDocs.size(); ++i)
    {
        ASSERT_EQ(string("ADD"), subDocs[i]->getField(CMD_TAG));
        ASSERT_EQ(string(""), subDocs[i]->getField(HA_RESERVED_SUB_MODIFY_FIELDS));
    }
    ASSERT_EQ(string("sub_1;sub_2"), subDocs[0]->getField(HA_RESERVED_MODIFY_FIELDS));
    ASSERT_EQ(string("sub_2"), subDocs[1]->getField(HA_RESERVED_MODIFY_FIELDS));

    ASSERT_EQ(string("ADD"), rawDoc->getField(CMD_TAG));
    ASSERT_EQ(string("field_1;field_2;sub_1;sub_2"),
              rawDoc->getField(HA_RESERVED_MODIFY_FIELDS));
    ASSERT_EQ(string("sub_1;sub_2sub_2"),
              rawDoc->getField(HA_RESERVED_SUB_MODIFY_FIELDS));
}

}
}
