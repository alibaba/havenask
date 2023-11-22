#include "indexlib/document/normal/SourceFormatter.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/document/normal/SerializedSourceDocument.h"
#include "indexlib/document/normal/SourceDocument.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"
#include "unittest/unittest.h"
using autil::StringUtil;
using autil::StringView;
using indexlib::document::SerializedSourceDocument;
using indexlib::document::SourceDocument;
using indexlibv2::config::MutableJson;
using std::string;

namespace indexlibv2::document {

class SourceFormatterTest : public TESTBASE
{
public:
    SourceFormatterTest() = default;
    ~SourceFormatterTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void SourceFormatterTest::setUp() {}

void SourceFormatterTest::tearDown() {}

TEST_F(SourceFormatterTest, TestSimpleProcess)
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
    std::shared_ptr<config::SourceIndexConfig> sourceSchema(new config::SourceIndexConfig);
    auto any = autil::legacy::json::ParseJson(configStr);
    MutableJson mtJson;
    config::IndexConfigDeserializeResource resource({}, mtJson);
    sourceSchema->Deserialize(any, 0, resource);

    // test formatter for non-state
    SourceFormatter formatter;
    formatter.Init(sourceSchema);
    autil::mem_pool::Pool* pool = new autil::mem_pool::Pool(1024);
    {
        std::shared_ptr<SourceDocument> srcDoc(new SourceDocument(pool));
        srcDoc->Append(0, "f1", StringView("v1"), true);
        srcDoc->Append(0, "f2", StringView("v2"), true);
        srcDoc->Append(1, "f1", StringView("v1"), true);
        srcDoc->Append(1, "f3", StringView("v3"), true);
        srcDoc->Append(1, "f3", StringView("v3"), true);
        srcDoc->AppendAccessaryField("a1", StringView("a1"), true);
        srcDoc->AppendAccessaryField("a2", StringView("a2"), true);

        std::shared_ptr<SerializedSourceDocument> serDoc(new SerializedSourceDocument());
        ASSERT_TRUE(formatter.SerializeSourceDocument(srcDoc, pool, serDoc).IsOK());

        std::shared_ptr<SerializedSourceDocument> serDoc2(new SerializedSourceDocument());
        srcDoc->Append(1, "f3", StringView("nv3"), true);
        srcDoc->AppendAccessaryField("a3", StringView("a3"), true);
        ASSERT_TRUE(formatter.SerializeSourceDocument(srcDoc, pool, serDoc2).IsOK());

        std::shared_ptr<SerializedSourceDocument> serDoc3(new SerializedSourceDocument());
        srcDoc->Append(0, "f1", StringView("nv1"), true);
        srcDoc->Append(1, "f1", StringView("nv1"), true);
        srcDoc->AppendAccessaryField("a2", StringView("na2"), true);
        ASSERT_TRUE(formatter.SerializeSourceDocument(srcDoc, pool, serDoc3).IsOK());

        // deserialize out of order
        SourceDocument result3(pool);
        ASSERT_TRUE(formatter.DeserializeSourceDocument(serDoc3.get(), &result3).IsOK());
        ASSERT_EQ(StringView("nv1"), result3.GetField(0, "f1"));
        ASSERT_EQ(StringView("v2"), result3.GetField(0, "f2"));
        ASSERT_EQ(StringView("nv1"), result3.GetField(1, "f1"));
        ASSERT_EQ(StringView("nv3"), result3.GetField(1, "f3"));
        ASSERT_EQ(StringView("na2"), result3.GetAccessaryField("a2"));
        ASSERT_EQ(StringView("a3"), result3.GetAccessaryField("a3"));
        ASSERT_EQ(StringView("a1"), result3.GetAccessaryField("a1"));

        SourceDocument result(pool);
        ASSERT_TRUE(formatter.DeserializeSourceDocument(serDoc.get(), &result).IsOK());
        ASSERT_EQ(StringView("v1"), result.GetField(0, "f1"));
        ASSERT_EQ(StringView("v2"), result.GetField(0, "f2"));
        ASSERT_EQ(StringView("v1"), result.GetField(1, "f1"));
        ASSERT_EQ(StringView("v3"), result.GetField(1, "f3"));
        ASSERT_EQ(StringView("a2"), result.GetAccessaryField("a2"));
        ASSERT_EQ(StringView("a1"), result.GetAccessaryField("a1"));
        ASSERT_EQ(StringView::empty_instance(), result.GetAccessaryField("a3"));

        SourceDocument result2(pool);
        ASSERT_TRUE(formatter.DeserializeSourceDocument(serDoc2.get(), &result2).IsOK());
        ASSERT_EQ(StringView("v1"), result2.GetField(0, "f1"));
        ASSERT_EQ(StringView("v2"), result2.GetField(0, "f2"));
        ASSERT_EQ(StringView("v1"), result2.GetField(1, "f1"));
        ASSERT_EQ(StringView("nv3"), result2.GetField(1, "f3"));
        ASSERT_EQ(StringView("a2"), result2.GetAccessaryField("a2"));
        ASSERT_EQ(StringView("a1"), result2.GetAccessaryField("a1"));
        ASSERT_EQ(StringView("a3"), result2.GetAccessaryField("a3"));
    }
    {
        std::shared_ptr<SourceDocument> srcDoc(new SourceDocument(pool));
        srcDoc->Append(0, "f1", StringView("c1"), true);
        srcDoc->Append(0, "f2", StringView("c2"), true);
        srcDoc->Append(1, "f1", StringView("c1"), true);
        srcDoc->Append(1, "f3", StringView("c3"), true);
        srcDoc->AppendAccessaryField("a3", StringView("a3"), true);
        std::shared_ptr<SerializedSourceDocument> serDoc(new SerializedSourceDocument());
        ASSERT_TRUE(formatter.SerializeSourceDocument(srcDoc, pool, serDoc).IsOK());

        SourceDocument result(pool);
        ASSERT_TRUE(formatter.DeserializeSourceDocument(serDoc.get(), &result).IsOK());
        ASSERT_EQ(StringView("c1"), result.GetField(0, "f1"));
        ASSERT_EQ(StringView("c2"), result.GetField(0, "f2"));
        ASSERT_EQ(StringView("c1"), result.GetField(1, "f1"));
        ASSERT_EQ(StringView("c3"), result.GetField(1, "f3"));
        ASSERT_EQ(StringView("a3"), result.GetAccessaryField("a3"));

        std::shared_ptr<RawDocument> rawDoc(new DefaultRawDocument);
        result.ToRawDocument(*rawDoc);
        ASSERT_EQ(StringView("c1"), rawDoc->getField("f1"));
        ASSERT_EQ(StringView("c2"), rawDoc->getField("f2"));
        ASSERT_EQ(StringView("c1"), rawDoc->getField("f1"));
        ASSERT_EQ(StringView("c3"), rawDoc->getField("f3"));
        ASSERT_EQ(StringView("a3"), rawDoc->getField("a3"));
    }
    delete pool;
}

TEST_F(SourceFormatterTest, TestEmptySourceDocument)
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
    std::shared_ptr<config::SourceIndexConfig> sourceSchema(new config::SourceIndexConfig);
    auto any = autil::legacy::json::ParseJson(configStr);
    MutableJson mtJson;
    config::IndexConfigDeserializeResource resource({}, mtJson);
    sourceSchema->Deserialize(any, 0, resource);

    // test formatter for non-state
    SourceFormatter formatter;
    formatter.Init(sourceSchema);
    autil::mem_pool::Pool* pool = new autil::mem_pool::Pool(1024);
    {
        std::shared_ptr<SourceDocument> srcDoc(new SourceDocument(pool));
        std::shared_ptr<SerializedSourceDocument> serDoc(new SerializedSourceDocument());
        ASSERT_TRUE(formatter.SerializeSourceDocument(srcDoc, pool, serDoc).IsOK());
        SourceDocument result(pool);
        ASSERT_TRUE(formatter.DeserializeSourceDocument(serDoc.get(), &result).IsOK());
    }
    delete pool;
}

TEST_F(SourceFormatterTest, TestSerializeWithNonExistField)
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
    std::shared_ptr<config::SourceIndexConfig> sourceSchema(new config::SourceIndexConfig);
    auto any = autil::legacy::json::ParseJson(configStr);
    MutableJson mtJson;
    config::IndexConfigDeserializeResource resource({}, mtJson);
    sourceSchema->Deserialize(any, 0, resource);

    // test formatter for non-state
    SourceFormatter formatter;
    formatter.Init(sourceSchema);

    autil::mem_pool::Pool* pool = new autil::mem_pool::Pool(1024);
    std::shared_ptr<SourceDocument> srcDoc(new SourceDocument(pool));
    auto MakeGroupData = [&srcDoc](index::sourcegroupid_t groupId, uint32_t fieldCount) {
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
    std::shared_ptr<SerializedSourceDocument> serDoc(new SerializedSourceDocument());
    ASSERT_TRUE(formatter.SerializeSourceDocument(srcDoc, pool, serDoc).IsOK());

    autil::DataBuffer buffer;
    serDoc->serialize(buffer);
    string encodeStr = string(buffer.getData(), buffer.getDataLen());

    autil::DataBuffer dataBuffer((void*)encodeStr.data(), encodeStr.size());
    serDoc.reset(new SerializedSourceDocument);
    serDoc->deserialize(dataBuffer, pool);

    SourceDocument result(pool);
    ASSERT_TRUE(formatter.DeserializeSourceDocument(serDoc.get(), &result).IsOK());

    auto CheckGroupData = [&result](index::sourcegroupid_t groupId, uint32_t fieldCount) {
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

    std::shared_ptr<RawDocument> rawDoc(new DefaultRawDocument);
    result.ToRawDocument(*rawDoc);

    auto CheckRawDoc = [&rawDoc](index::sourcegroupid_t groupId, uint32_t fieldCount) {
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

} // namespace indexlibv2::document
