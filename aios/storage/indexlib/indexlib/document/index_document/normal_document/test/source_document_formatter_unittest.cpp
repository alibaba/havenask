#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/source_document_formatter.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;
using autil::StringView;
using autil::legacy::Any;
namespace indexlib { namespace document {

class SourceDocumentFormatterTest : public INDEXLIB_TESTBASE
{
public:
    SourceDocumentFormatterTest();
    ~SourceDocumentFormatterTest();

    DECLARE_CLASS_NAME(SourceDocumentFormatterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestEmptySourceDocument();
    void TestSerializeWithNonExistField();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SourceDocumentFormatterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SourceDocumentFormatterTest, TestEmptySourceDocument);
INDEXLIB_UNIT_TEST_CASE(SourceDocumentFormatterTest, TestSerializeWithNonExistField);

IE_LOG_SETUP(document, SourceDocumentFormatterTest);

SourceDocumentFormatterTest::SourceDocumentFormatterTest() {}

SourceDocumentFormatterTest::~SourceDocumentFormatterTest() {}

void SourceDocumentFormatterTest::CaseSetUp() {}

void SourceDocumentFormatterTest::CaseTearDown() {}

void SourceDocumentFormatterTest::TestSimpleProcess()
{
    string configStr = R"(
    {
        "group_configs": [
            {
                "field_mode": "specified_field",
                "fields": ["f1", "f2"]
            },
            {
                "field_mode": "specified_field",
                "fields": ["f1", "f3"],
                "parameter" : {
                    "compress_type": "uniq|equal",
                    "doc_compressor": "zlib",
                    "file_compressor" : "snappy",
                    "file_compress_buffer_size" : 8192
                }
            }
        ]
    })";
    config::SourceSchemaPtr sourceSchema(new config::SourceSchema);
    Any any = autil::legacy::json::ParseJson(configStr);
    autil::legacy::FromJson(*(sourceSchema.get()), any);

    // test formatter for non-state
    SourceDocumentFormatter formatter;
    formatter.Init(sourceSchema);
    autil::mem_pool::Pool* pool = new autil::mem_pool::Pool(1024);
    {
        SourceDocumentPtr srcDoc(new SourceDocument(pool));
        srcDoc->Append(0, "f1", StringView("v1"), true);
        srcDoc->Append(0, "f2", StringView("v2"), true);
        srcDoc->Append(1, "f1", StringView("v1"), true);
        srcDoc->Append(1, "f3", StringView("v3"), true);
        srcDoc->Append(1, "f3", StringView("v3"), true);
        srcDoc->AppendAccessaryField("a1", StringView("a1"), true);
        srcDoc->AppendAccessaryField("a2", StringView("a2"), true);

        SerializedSourceDocumentPtr serDoc(new SerializedSourceDocument());
        formatter.SerializeSourceDocument(srcDoc, pool, serDoc);

        SerializedSourceDocumentPtr serDoc2(new SerializedSourceDocument());
        srcDoc->Append(1, "f3", StringView("nv3"), true);
        srcDoc->AppendAccessaryField("a3", StringView("a3"), true);
        formatter.SerializeSourceDocument(srcDoc, pool, serDoc2);

        SerializedSourceDocumentPtr serDoc3(new SerializedSourceDocument());
        srcDoc->Append(0, "f1", StringView("nv1"), true);
        srcDoc->Append(1, "f1", StringView("nv1"), true);
        srcDoc->AppendAccessaryField("a2", StringView("na2"), true);
        formatter.SerializeSourceDocument(srcDoc, pool, serDoc3);

        // deserialize out of order
        SourceDocument result3(pool);
        formatter.DeserializeSourceDocument(serDoc3, &result3);
        ASSERT_EQ(StringView("nv1"), result3.GetField(0, "f1"));
        ASSERT_EQ(StringView("v2"), result3.GetField(0, "f2"));
        ASSERT_EQ(StringView("nv1"), result3.GetField(1, "f1"));
        ASSERT_EQ(StringView("nv3"), result3.GetField(1, "f3"));
        ASSERT_EQ(StringView("na2"), result3.GetAccessaryField("a2"));
        ASSERT_EQ(StringView("a3"), result3.GetAccessaryField("a3"));
        ASSERT_EQ(StringView("a1"), result3.GetAccessaryField("a1"));

        SourceDocument result(pool);
        formatter.DeserializeSourceDocument(serDoc, &result);
        ASSERT_EQ(StringView("v1"), result.GetField(0, "f1"));
        ASSERT_EQ(StringView("v2"), result.GetField(0, "f2"));
        ASSERT_EQ(StringView("v1"), result.GetField(1, "f1"));
        ASSERT_EQ(StringView("v3"), result.GetField(1, "f3"));
        ASSERT_EQ(StringView("a2"), result.GetAccessaryField("a2"));
        ASSERT_EQ(StringView("a1"), result.GetAccessaryField("a1"));
        ASSERT_EQ(StringView::empty_instance(), result.GetAccessaryField("a3"));

        SourceDocument result2(pool);
        formatter.DeserializeSourceDocument(serDoc2, &result2);
        ASSERT_EQ(StringView("v1"), result2.GetField(0, "f1"));
        ASSERT_EQ(StringView("v2"), result2.GetField(0, "f2"));
        ASSERT_EQ(StringView("v1"), result2.GetField(1, "f1"));
        ASSERT_EQ(StringView("nv3"), result2.GetField(1, "f3"));
        ASSERT_EQ(StringView("a2"), result2.GetAccessaryField("a2"));
        ASSERT_EQ(StringView("a1"), result2.GetAccessaryField("a1"));
        ASSERT_EQ(StringView("a3"), result2.GetAccessaryField("a3"));
    }
    {
        SourceDocumentPtr srcDoc(new SourceDocument(pool));
        srcDoc->Append(0, "f1", StringView("c1"), true);
        srcDoc->Append(0, "f2", StringView("c2"), true);
        srcDoc->Append(1, "f1", StringView("c1"), true);
        srcDoc->Append(1, "f3", StringView("c3"), true);
        srcDoc->AppendAccessaryField("a3", StringView("a3"), true);
        SerializedSourceDocumentPtr serDoc(new SerializedSourceDocument());
        formatter.SerializeSourceDocument(srcDoc, pool, serDoc);

        SourceDocument result(pool);
        formatter.DeserializeSourceDocument(serDoc, &result);
        ASSERT_EQ(StringView("c1"), result.GetField(0, "f1"));
        ASSERT_EQ(StringView("c2"), result.GetField(0, "f2"));
        ASSERT_EQ(StringView("c1"), result.GetField(1, "f1"));
        ASSERT_EQ(StringView("c3"), result.GetField(1, "f3"));
        ASSERT_EQ(StringView("a3"), result.GetAccessaryField("a3"));

        RawDocumentPtr rawDoc(new DefaultRawDocument);
        result.ToRawDocument(*rawDoc);
        ASSERT_EQ(StringView("c1"), rawDoc->getField("f1"));
        ASSERT_EQ(StringView("c2"), rawDoc->getField("f2"));
        ASSERT_EQ(StringView("c1"), rawDoc->getField("f1"));
        ASSERT_EQ(StringView("c3"), rawDoc->getField("f3"));
        ASSERT_EQ(StringView("a3"), rawDoc->getField("a3"));
    }
    delete pool;
}

void SourceDocumentFormatterTest::TestEmptySourceDocument()
{
    string configStr = R"(
    {
        "group_configs": [
            {
                "field_mode": "specified_field",
                "fields": ["f1", "f2"]
            },
            {
                "field_mode": "specified_field",
                "fields": ["f1", "f3"],
                "parameter" : {
                    "compress_type": "uniq|equal",
                    "doc_compressor": "zlib",
                    "file_compressor" : "snappy",
                    "file_compress_buffer_size" : 8192
                }
            }
        ]
    })";
    config::SourceSchemaPtr sourceSchema(new config::SourceSchema);
    Any any = autil::legacy::json::ParseJson(configStr);
    autil::legacy::FromJson(*(sourceSchema.get()), any);

    // test formatter for non-state
    SourceDocumentFormatter formatter;
    formatter.Init(sourceSchema);
    autil::mem_pool::Pool* pool = new autil::mem_pool::Pool(1024);
    {
        SourceDocumentPtr srcDoc(new SourceDocument(pool));
        SerializedSourceDocumentPtr serDoc(new SerializedSourceDocument());
        formatter.SerializeSourceDocument(srcDoc, pool, serDoc);
        SourceDocument result(pool);
        formatter.DeserializeSourceDocument(serDoc, &result);
    }
    delete pool;
}

void SourceDocumentFormatterTest::TestSerializeWithNonExistField()
{
    string configStr = R"(
    {
        "group_configs": [
            {
                "field_mode": "specified_field",
                "fields": ["f1", "f2"]
            },
            {
                "field_mode": "specified_field",
                "fields": ["f1", "f3"],
                "parameter" : {
                    "compress_type": "uniq|equal",
                    "doc_compressor": "zlib",
                    "file_compressor" : "snappy",
                    "file_compress_buffer_size" : 8192
                }
            },
            {
                "field_mode": "specified_field",
                "fields": ["f1", "f2"]
            }
        ]
    })";
    config::SourceSchemaPtr sourceSchema(new config::SourceSchema);
    Any any = autil::legacy::json::ParseJson(configStr);
    autil::legacy::FromJson(*(sourceSchema.get()), any);

    // test formatter for non-state
    SourceDocumentFormatter formatter;
    formatter.Init(sourceSchema);

    autil::mem_pool::Pool* pool = new autil::mem_pool::Pool(1024);
    SourceDocumentPtr srcDoc(new SourceDocument(pool));
    auto MakeGroupData = [&srcDoc](index::groupid_t groupId, uint32_t fieldCount) {
        for (uint32_t i = 0; i < fieldCount; i++) {
            string fn = string("f") + StringUtil::toString(groupId) + StringUtil::toString(i);
            if (i % 3 == 1) {
                srcDoc->AppendNonExistField(groupId, fn);
                continue;
            }
            string value = string("v") + StringUtil::toString(groupId) + StringUtil::toString(i);
            srcDoc->Append(groupId, fn, StringView(value), true);
        }
    };

    MakeGroupData(0, 7);
    MakeGroupData(1, 5);
    MakeGroupData(2, 9);

    srcDoc->AppendNonExistAccessaryField("a4");
    srcDoc->AppendAccessaryField("a3", StringView("a3"), true);
    SerializedSourceDocumentPtr serDoc(new SerializedSourceDocument());
    formatter.SerializeSourceDocument(srcDoc, pool, serDoc);

    autil::DataBuffer buffer;
    serDoc->serialize(buffer);
    string encodeStr = string(buffer.getData(), buffer.getDataLen());

    autil::DataBuffer dataBuffer((void*)encodeStr.data(), encodeStr.size());
    serDoc.reset(new SerializedSourceDocument);
    serDoc->deserialize(dataBuffer, pool);

    SourceDocument result(pool);
    formatter.DeserializeSourceDocument(serDoc, &result);

    auto CheckGroupData = [&result](index::groupid_t groupId, uint32_t fieldCount) {
        for (uint32_t i = 0; i < fieldCount; i++) {
            string fn = string("f") + StringUtil::toString(groupId) + StringUtil::toString(i);

            if (i % 3 == 1) {
                auto field = result.GetField(groupId, fn);
                ASSERT_EQ(autil::StringView::empty_instance(), field);
                ASSERT_TRUE(result.HasField(groupId, fn));
                ASSERT_TRUE(result.IsNonExistField(groupId, fn));
                continue;
            }
            string value = string("v") + StringUtil::toString(groupId) + StringUtil::toString(i);
            auto field = result.GetField(groupId, fn);
            ASSERT_EQ(autil::StringView(value), field);
        }
    };

    CheckGroupData(0, 7);
    CheckGroupData(1, 5);
    CheckGroupData(2, 9);

    ASSERT_EQ(autil::StringView::empty_instance(), srcDoc->GetAccessaryField("a4"));
    ASSERT_EQ(StringView("a3"), srcDoc->GetAccessaryField("a3"));

    RawDocumentPtr rawDoc(new DefaultRawDocument);
    result.ToRawDocument(*rawDoc);

    auto CheckRawDoc = [&rawDoc](index::groupid_t groupId, uint32_t fieldCount) {
        for (uint32_t i = 0; i < fieldCount; i++) {
            string fn = string("f") + StringUtil::toString(groupId) + StringUtil::toString(i);

            if (i % 3 == 1) {
                ASSERT_FALSE(rawDoc->exist(fn));
                continue;
            }
            string value = string("v") + StringUtil::toString(groupId) + StringUtil::toString(i);
            auto field = rawDoc->getField(fn);
            ASSERT_EQ(StringView(value), field);
        }
    };

    CheckRawDoc(0, 7);
    CheckRawDoc(1, 5);
    CheckRawDoc(2, 9);

    ASSERT_FALSE(rawDoc->exist("a4"));
    ASSERT_EQ(StringView("a3"), rawDoc->getField("a3"));
    delete pool;
}
}} // namespace indexlib::document
