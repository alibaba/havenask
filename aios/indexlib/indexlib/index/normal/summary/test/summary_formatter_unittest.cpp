#include <vector>
#include <string>
#include <sstream>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/index/test/partition_schema_maker.h"

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(index);
using namespace std;

class SummaryFormatterTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SummaryFormatterTest);
public:
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForSerializeSummaryWithCompress()
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, "title:text;user_name:string;user_id:integer;price:float", 
                "", "", "title;price");
        schema->GetRegionSchema(DEFAULT_REGIONID)->SetSummaryCompress(true);
        docid_t docId = 0;
        int beginContentId = 0;
        string expectedSummaryString;
        autil::mem_pool::Pool pool;
        SummaryDocumentPtr document = MakeOneDocument(
                docId, beginContentId, &pool, schema, expectedSummaryString);

        SummarySchemaPtr summarySchema = schema->GetSummarySchema();
        INDEXLIB_TEST_TRUE(summarySchema->GetSummaryGroupConfig(DEFAULT_SUMMARYGROUPID)->IsCompress());
        SummaryFormatter formatter(summarySchema);

        size_t serializedLen = formatter.GetSerializeLength(document);
        char* serialize = new char[serializedLen];
        formatter.SerializeSummary(document, serialize, serializedLen);

        string serializeString = string(serialize, serializedLen);
        delete[] serialize;

        SearchSummaryDocumentPtr deserializedDoc(new SearchSummaryDocument(NULL, summarySchema->GetSummaryCount()));
        formatter.TEST_DeserializeSummary(deserializedDoc.get(),
                serializeString.c_str(), serializeString.size());

        INDEXLIB_TEST_TRUE(deserializedDoc != NULL);
        INDEXLIB_TEST_EQUAL(document->GetNotEmptyFieldCount(), deserializedDoc->GetFieldCount());
        for (uint32_t j = 0; j < document->GetNotEmptyFieldCount(); ++j)
        {
            const autil::ConstString &expectField = document->GetField((fieldid_t)j);
            summaryfieldid_t summaryFieldId = summarySchema->GetSummaryFieldId((fieldid_t)j);
            const autil::ConstString* str = deserializedDoc->GetFieldValue(summaryFieldId);
            if (NULL == str)
            {
                INDEXLIB_TEST_TRUE(expectField.empty());
                continue;
            }
            INDEXLIB_TEST_EQUAL(expectField, *str);
        }
    }

    void TestCaseForSerializeSummaryWithOutCompress()
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, "title:text;user_name:string;user_id:integer;price:float", 
                "", "", "title;price");

        schema->GetRegionSchema(DEFAULT_REGIONID)->SetSummaryCompress(false);
        docid_t docId = 0;
        int beginContentId = 0;
        string expectedSummaryString;
        autil::mem_pool::Pool pool;
        SummaryDocumentPtr document = MakeOneDocument(
                docId, beginContentId, &pool, schema, expectedSummaryString);

        SummaryFormatter formatter(schema->GetSummarySchema());
        char* value;
        size_t summaryLen ;
        
        summaryLen = formatter.GetSerializeLength(document);
        value = new char[summaryLen];
        formatter.SerializeSummary(document, value, summaryLen);

        // compare total size
        uint32_t valueLen = string("content_1").size();
        size_t expectedSize = 2 * (SummaryGroupFormatter::GetVUInt32Length(valueLen) + valueLen);
        INDEXLIB_TEST_EQUAL(expectedSize, summaryLen);

        const char* cursor = value;
        // compare first field
        //compare length of field value
        string expectedContent = "content_0";
        valueLen = expectedContent.size();
        INDEXLIB_TEST_EQUAL(valueLen, SummaryGroupFormatter::ReadVUInt32(cursor));
        //compare value of field.
        INDEXLIB_TEST_EQUAL(expectedContent, string(cursor, valueLen));
        cursor += valueLen;

        // compare second field
        expectedContent = "content_3";        
        valueLen = expectedContent.size();
        INDEXLIB_TEST_EQUAL(valueLen, SummaryGroupFormatter::ReadVUInt32(cursor));
        INDEXLIB_TEST_EQUAL(expectedContent, string(cursor, valueLen));
        cursor += valueLen;

        delete []value;
    }

    void TestCaseForDeserializeSummaryOK()
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, "title:text;user_name:string;user_id:integer;price:float", 
                "", "", "title;price");
        SummaryFormatter formatter(schema->GetSummarySchema());

        const char* content1 = "1234567890";
        uint32_t contentLen1 = strlen(content1);
        uint32_t vintLen1 = SummaryGroupFormatter::GetVUInt32Length(contentLen1);
        const char* content2 = "abcdefghijklmnopqrstuvwxyz";
        uint32_t contentLen2 = strlen(content2);
        uint32_t vintLen2 = SummaryGroupFormatter::GetVUInt32Length(contentLen2);
        uint32_t totalLen = vintLen1 + contentLen1 + vintLen2 + contentLen2;

        char* buffer = new char[totalLen];
        char* pin = buffer;
        SummaryGroupFormatter::WriteVUInt32(contentLen1, pin);
        memcpy(pin, content1, contentLen1);
        pin += contentLen1;
        SummaryGroupFormatter::WriteVUInt32(contentLen2, pin);
        memcpy(pin, content2, contentLen2);
        pin += contentLen2;

        string value(buffer, totalLen);
        
        const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();
        SearchSummaryDocumentPtr document(new SearchSummaryDocument(NULL, summarySchema->GetSummaryCount()));
        formatter.TEST_DeserializeSummary(document.get(), value.c_str(), value.size());
        INDEXLIB_TEST_TRUE(document.get() != NULL);
        INDEXLIB_TEST_EQUAL((size_t)2, document->GetFieldCount());

        // test field 0
        summaryfieldid_t summaryFieldId = summarySchema->GetSummaryFieldId((fieldid_t)0);
        const autil::ConstString* fieldValue = document->GetFieldValue(summaryFieldId);
        INDEXLIB_TEST_EQUAL(string(content1), string(fieldValue->data(), fieldValue->size()));

        // test field 3
        summaryFieldId = summarySchema->GetSummaryFieldId((fieldid_t)3);
        fieldValue = document->GetFieldValue(summaryFieldId);
        INDEXLIB_TEST_EQUAL(string(content2), string(fieldValue->data(), fieldValue->size()));

        // test not exist field
        summaryFieldId = summarySchema->GetSummaryFieldId((fieldid_t)1);
        fieldValue = document->GetFieldValue(summaryFieldId);
        INDEXLIB_TEST_TRUE(fieldValue == NULL);

        summaryFieldId = summarySchema->GetSummaryFieldId((fieldid_t)2);
        fieldValue = document->GetFieldValue(summaryFieldId);
        INDEXLIB_TEST_TRUE(fieldValue == NULL);

        delete []buffer;
    }

    void TestCaseForDeserializeSummaryFail()
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, "title:text;user_name:string;user_id:integer;price:float", 
                "", "", "title;price");
        SummaryFormatter formatter(schema->GetSummarySchema());
        
        const char* content1 = "1234567890";
        uint32_t contentLen1 = strlen(content1);
        uint32_t vintLen1 = SummaryGroupFormatter::GetVUInt32Length(contentLen1);
        const char* content2 = "abcdefghijklmnopqrstuvwxyz";
        uint32_t contentLen2 = strlen(content2);
        uint32_t vintLen2 = SummaryGroupFormatter::GetVUInt32Length(contentLen2);
        uint32_t totalLen = vintLen1 + contentLen1 + vintLen2 + contentLen2;

        char* buffer = new char[totalLen];
        char* pin = buffer;
        SummaryGroupFormatter::WriteVUInt32(contentLen1, pin);
        memcpy(pin, content1, contentLen1);
        pin += contentLen1;
        SummaryGroupFormatter::WriteVUInt32(contentLen2, pin);
        memcpy(pin, content2, contentLen2);
        pin += contentLen2;

        string value(buffer, totalLen);

        const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();
        SearchSummaryDocumentPtr document(new SearchSummaryDocument(NULL, summarySchema->GetSummaryCount()));

        // good case: 5 Empty Fields
        string goodValue("\0\0\0\0\0", 5);
        bool success = formatter.TEST_DeserializeSummary(document.get(), goodValue.c_str(), goodValue.size());
        INDEXLIB_TEST_TRUE(success);

        // have field value len, no field value
        string badValue = value.substr(0, vintLen1);
        success = formatter.TEST_DeserializeSummary(document.get(), badValue.c_str(), badValue.size());
        INDEXLIB_TEST_TRUE(!success);

        // empty content
        badValue = "";
        success = formatter.TEST_DeserializeSummary(document.get(), badValue.c_str(), badValue.size());
        INDEXLIB_TEST_TRUE(!success);
       
        // actual First field value len > input total length
        badValue = "|123456";
        success = formatter.TEST_DeserializeSummary(document.get(), badValue.c_str(), 16);
        INDEXLIB_TEST_TRUE(!success);
       
        // actual content len > input total length
        badValue = value;
        success = formatter.TEST_DeserializeSummary(document.get(), badValue.c_str(), 0);
        INDEXLIB_TEST_TRUE(!success);
       
        // actual Second field value len < input Second field value len
        badValue = value.substr(0, value.size() - 1);
        success = formatter.TEST_DeserializeSummary(document.get(), badValue.c_str(), badValue.size());
        INDEXLIB_TEST_TRUE(!success);

        // actual Second field value len > input Second field value len
        badValue = value + "z";
        success = formatter.TEST_DeserializeSummary(document.get(), badValue.c_str(), badValue.size());
        INDEXLIB_TEST_TRUE(!success);

        delete []buffer;
    }

    void TestCaseForWriteVUInt32AndReadVUInt32()
    {
        CheckForWriteVUInt32AndReadVUInt32(0, 1);
        CheckForWriteVUInt32AndReadVUInt32(127, 1);
        CheckForWriteVUInt32AndReadVUInt32(128, 2);
        CheckForWriteVUInt32AndReadVUInt32(16383, 2);
        CheckForWriteVUInt32AndReadVUInt32(16384, 3);
        CheckForWriteVUInt32AndReadVUInt32(2097151, 3);
        CheckForWriteVUInt32AndReadVUInt32(2097152, 4);
        CheckForWriteVUInt32AndReadVUInt32(268435455, 4);
        CheckForWriteVUInt32AndReadVUInt32(268435456, 5);
        CheckForWriteVUInt32AndReadVUInt32(4294967295, 5);
    }

    IndexPartitionSchemaPtr MakeIndexPartitionSchema(
            const vector<string>& fieldNames, 
            const vector<FieldType>& fieldTypes, 
            const vector<bool>& fieldNeedStore)
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));

        // make fieldSchema
        size_t i;
        for (i = 0; i < fieldNames.size(); ++i)
        {
            schema->AddFieldConfig(fieldNames[i], fieldTypes[i]);
        }

        // make SummarySchema
        for (i = 0; i < fieldNames.size(); ++i)
        {
            if (fieldNeedStore[i]) 
            {
                schema->AddSummaryConfig(fieldNames[i]);
            }
        }
        return schema;
    }

    SummaryDocumentPtr MakeOneDocument(docid_t docId, int beginContentId, 
            autil::mem_pool::Pool *pool,
            const IndexPartitionSchemaPtr &schema, 
            string& expectedSummaryString)
    {
        SummaryDocumentPtr document(new SummaryDocument());
        document->Reserve(docId);
        expectedSummaryString.clear();
        FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
        for (uint32_t i = 0; i < fieldSchema->GetFieldCount(); ++i)
        {
            if (!schema->GetSummarySchema()->IsInSummary((fieldid_t)i))
            {
                continue;
            }
            const FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig((fieldid_t)i);
            string content = "content_" + Int32ToStr(i + beginContentId);
            document->SetField(fieldConfig->GetFieldId(), autil::ConstString(content, pool));
        }
        return document;
    }

    string Int32ToStr(int32_t value)
    {
        stringstream stream;
        stream << value;
        return stream.str();
    }

    void CheckForWriteVUInt32AndReadVUInt32(uint32_t checkNumber, uint32_t vintLen)
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, "title:text;user_name:string;user_id:integer;price:float", 
                "", "", "title;price");
        SummaryFormatter formatter(schema->GetSummarySchema());

        INDEXLIB_TEST_EQUAL(vintLen, SummaryGroupFormatter::GetVUInt32Length(checkNumber));

        char buffer[10] = {0};
        char* pin = buffer;
        const char* pout = buffer;
        SummaryGroupFormatter::WriteVUInt32(checkNumber, pin);
        INDEXLIB_TEST_TRUE(pin == buffer + vintLen);
        INDEXLIB_TEST_EQUAL(checkNumber, SummaryGroupFormatter::ReadVUInt32(pout));
        INDEXLIB_TEST_TRUE(pout == buffer + vintLen);
        for (size_t i = vintLen; i < sizeof(buffer); i++)
        {
            INDEXLIB_TEST_EQUAL(0, buffer[i]);
        }
    }

private:
};

INDEXLIB_UNIT_TEST_CASE(SummaryFormatterTest, TestCaseForSerializeSummaryWithOutCompress);
INDEXLIB_UNIT_TEST_CASE(SummaryFormatterTest, TestCaseForSerializeSummaryWithCompress);
INDEXLIB_UNIT_TEST_CASE(SummaryFormatterTest, TestCaseForDeserializeSummaryOK);
INDEXLIB_UNIT_TEST_CASE(SummaryFormatterTest, TestCaseForDeserializeSummaryFail);
INDEXLIB_UNIT_TEST_CASE(SummaryFormatterTest, TestCaseForWriteVUInt32AndReadVUInt32);

IE_NAMESPACE_END(index);
