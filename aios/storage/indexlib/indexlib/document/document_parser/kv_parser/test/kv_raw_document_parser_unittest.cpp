#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/common/field_format/attribute/string_attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/document/document_parser/kv_parser/kv_raw_document_parser.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/document/kv_document/kv_message_generated.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::test;

namespace indexlib { namespace document {

class KvRawDocumentParserTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp();
    void CaseTearDown();
};

void KvRawDocumentParserTest::CaseSetUp() {}

void KvRawDocumentParserTest::CaseTearDown() {}

TEST_F(KvRawDocumentParserTest, testKV)
{
    string schemaStr = R"(
{
    "attributes": ["userid", "price"],
    "fields": [
        { "field_name": "itemid", "field_type": "INT64", "multi_value": false },
        { "field_name": "userid", "field_type": "INT64", "multi_value": false },
        { "field_name": "price", "field_type": "INT64", "multi_value": false }
    ],
    "indexs": [ {"index_fields": "itemid", "index_name": "itemid", "index_type": "PRIMARY_KEY"} ],
    "table_name": "test",
    "table_type": "kv"
})";
    auto schema = std::make_shared<IndexPartitionSchema>();
    FromJsonString(*schema, schemaStr);
    string rawData = "userid=100;itemid=1;price=99;CMD=add";
    KvRawDocumentParser parser(schema);
    ASSERT_TRUE(parser.construct(DocumentInitParamPtr()));
    document::KVDocument doc;
    RawDocumentParser::Message msg;
    msg.data = rawData;
    msg.timestamp = 100;
    ASSERT_TRUE(parser.parse(msg, doc));

    auto kvIndexDoc = doc.begin();
    ASSERT_EQ(1u, kvIndexDoc->GetPKeyHash());
    ASSERT_FALSE(kvIndexDoc->HasSKey());
    ASSERT_EQ(100, doc.GetTimestamp());
    ASSERT_EQ(100, kvIndexDoc->GetTimestamp());

    ASSERT_EQ(ADD_DOC, kvIndexDoc->GetDocOperateType());
    auto value = kvIndexDoc->GetValue();
    ASSERT_EQ(25u, value.size());
    StringAttributeConvertor convertor;
    auto attrMeta = convertor.Decode(value);
    ASSERT_EQ(100, *(int64_t*)(attrMeta.data.data() + 1));
    ASSERT_EQ(99, *(int64_t*)(attrMeta.data.data() + 1 + sizeof(int64_t)));
}

TEST_F(KvRawDocumentParserTest, testKKV)
{
    string schemaStr = R"(
{
    "indexs": [
    {
        "index_fields": [
        { "key_type": "prefix", "field_name": "nid" },
        { "key_type": "suffix", "count_limits": 5000, "field_name": "uid" }
        ],
        "index_name": "nid_uid",
        "index_type": "PRIMARY_KEY"
    }
    ],
    "attributes": ["seller_id"],
    "fields": [
        { "multi_value": false, "field_type": "INT64", "field_name": "nid" },
        { "multi_value": false, "field_type": "INT64", "field_name": "uid" },
        { "multi_value": false, "field_type": "INT64", "field_name": "seller_id" }
    ],
    "table_name": "click",
    "table_type": "kkv"
}
)";
    auto schema = std::make_shared<IndexPartitionSchema>();
    FromJsonString(*schema, schemaStr);
    string rawData = "nid=100;uid=1;seller_id=99;CMD=add";
    KvRawDocumentParser parser(schema);
    ASSERT_TRUE(parser.construct(DocumentInitParamPtr()));
    document::KVDocument doc;
    RawDocumentParser::Message msg;
    msg.data = rawData;
    msg.timestamp = 100;
    ASSERT_TRUE(parser.parse(msg, doc));

    auto kvIndexDoc = doc.begin();
    ASSERT_EQ(100u, kvIndexDoc->GetPKeyHash());
    ASSERT_TRUE(kvIndexDoc->HasSKey());
    ASSERT_EQ(1u, kvIndexDoc->GetSKeyHash());
    ASSERT_EQ(100, doc.GetTimestamp());
    ASSERT_EQ(100, kvIndexDoc->GetTimestamp());

    ASSERT_EQ(ADD_DOC, kvIndexDoc->GetDocOperateType());
    auto value = kvIndexDoc->GetValue();
    ASSERT_EQ(17, value.size());
    StringAttributeConvertor convertor;
    auto attrMeta = convertor.Decode(value);
    ASSERT_EQ(99, *(int64_t*)(attrMeta.data.data() + 1));
}

TEST_F(KvRawDocumentParserTest, testFormatError)
{
    string schemaStr = R"(
{
    "attributes": ["userid", "price", "title"],
    "fields": [
        { "field_name": "itemid", "field_type": "INT64", "multi_value": false },
        { "field_name": "userid", "field_type": "INT64", "multi_value": false },
        { "field_name": "price", "field_type": "INT64", "multi_value": false },
        { "field_name": "title", "field_type": "STRING", "multi_value": false }
    ],
    "indexs": [ {"index_fields": "itemid", "index_name": "itemid", "index_type": "PRIMARY_KEY"} ],
    "table_name": "test",
    "table_type": "kv"
})";
    auto schema = std::make_shared<IndexPartitionSchema>();
    FromJsonString(*schema, schemaStr);
    KvRawDocumentParser parser(schema);
    auto param = std::make_shared<DocumentInitParam>();
    param->AddValue("tolerate_field_format_error", "false");
    ASSERT_TRUE(parser.construct(param));
    {
        // miss price and title
        document::KVDocument doc;
        RawDocumentParser::Message msg;
        string rawData = "userid=100;itemid=1;CMD=add";
        msg.data = rawData;
        msg.timestamp = 100;
        ASSERT_FALSE(parser.parse(msg, doc));
    }
    {
        document::KVDocument doc;
        RawDocumentParser::Message msg;
        string rawData = "userid=100;itemid=1;price=99;title=hello;CMD=add";
        msg.data = rawData;
        msg.timestamp = 100;
        ASSERT_TRUE(parser.parse(msg, doc));
        auto kvIndexDoc = doc.begin();
        ASSERT_EQ(1u, kvIndexDoc->GetPKeyHash());
        ASSERT_FALSE(kvIndexDoc->HasSKey());
        ASSERT_EQ(100, doc.GetTimestamp());
        ASSERT_EQ(100, kvIndexDoc->GetTimestamp());

        ASSERT_EQ(ADD_DOC, kvIndexDoc->GetDocOperateType());
        auto value = kvIndexDoc->GetValue();
        ASSERT_EQ(39u, value.size());
        StringAttributeConvertor convertor;
        auto attrMeta = convertor.Decode(value);
        ASSERT_EQ(100, *(int64_t*)(attrMeta.data.data() + 1));
        ASSERT_EQ(99, *(int64_t*)(attrMeta.data.data() + 1 + sizeof(int64_t)));
        std::string expected = "hello";
        std::string actual(attrMeta.data.data() + 1 + sizeof(int64_t) + sizeof(int64_t) + 8 + 1);
        ASSERT_EQ(expected, actual);
    }
    {
        // miss title
        document::KVDocument doc;
        RawDocumentParser::Message msg;
        string rawData = "userid=100;itemid=1;price=99;CMD=add";
        msg.data = rawData;
        msg.timestamp = 100;
        ASSERT_FALSE(parser.parse(msg, doc));
    }
    {
        // empty title string
        document::KVDocument doc;
        RawDocumentParser::Message msg;
        string rawData = "userid=100;itemid=1;price=99;title=;CMD=add";
        msg.data = rawData;
        msg.timestamp = 100;
        ASSERT_TRUE(parser.parse(msg, doc));
        auto kvIndexDoc = doc.begin();
        ASSERT_EQ(1u, kvIndexDoc->GetPKeyHash());
        ASSERT_FALSE(kvIndexDoc->HasSKey());
        ASSERT_EQ(100, doc.GetTimestamp());
        ASSERT_EQ(100, kvIndexDoc->GetTimestamp());

        ASSERT_EQ(ADD_DOC, kvIndexDoc->GetDocOperateType());
        auto value = kvIndexDoc->GetValue();
        ASSERT_EQ(34u, value.size()); // 8 + 1 + 8 + 8 + 8 + 1
        StringAttributeConvertor convertor;
        auto attrMeta = convertor.Decode(value);
        ASSERT_EQ(100, *(int64_t*)(attrMeta.data.data() + 1));
        ASSERT_EQ(99, *(int64_t*)(attrMeta.data.data() + 1 + sizeof(int64_t)));
        char* titleHeaderAddr = (char*)attrMeta.data.data() + 1 + sizeof(int64_t) + sizeof(int64_t) + 8 + 1;
        ASSERT_EQ(0, *titleHeaderAddr);
    }
}

TEST_F(KvRawDocumentParserTest, testMultiDocFormatError)
{
    string schemaStr = R"(
{
    "attributes": ["userid", "price", "title"],
    "fields": [
        { "field_name": "itemid", "field_type": "INT64", "multi_value": false },
        { "field_name": "userid", "field_type": "INT64", "multi_value": false },
        { "field_name": "price", "field_type": "INT64", "multi_value": false },
        { "field_name": "title", "field_type": "STRING", "multi_value": false }
    ],
    "indexs": [ {"index_fields": "itemid", "index_name": "itemid", "index_type": "PRIMARY_KEY"} ],
    "table_name": "test",
    "table_type": "kv"
})";
    auto schema = std::make_shared<IndexPartitionSchema>();
    FromJsonString(*schema, schemaStr);
    KvRawDocumentParser parser(schema);
    auto param = std::make_shared<DocumentInitParam>();
    param->AddValue("tolerate_field_format_error", "false");
    ASSERT_TRUE(parser.construct(param));
    {
        // single doc fail
        document::KVDocument doc;
        std::vector<RawDocumentParser::Message> msgs;
        {
            RawDocumentParser::Message msg;
            string rawData = "userid=100;itemid=1;price=99;title=hello;CMD=add";
            msg.data = rawData;
            msg.timestamp = 100;
            msgs.push_back(msg);
        }
        {
            RawDocumentParser::Message msg;
            string rawData = "userid=100;itemid=1;CMD=add";
            msg.data = rawData;
            msg.timestamp = 100;
            msgs.push_back(msg);
        }
        ASSERT_TRUE(parser.parseMultiMessage(msgs, doc));
        ASSERT_EQ(1u, doc.GetMessageCount());
        ASSERT_EQ(1u, doc.GetDocumentCount());
    }
    {
        // single doc fail
        document::KVDocument doc;
        std::vector<RawDocumentParser::Message> msgs;
        {
            RawDocumentParser::Message msg;
            string rawData = "userid=100;itemid=1;CMD=add";
            msg.data = rawData;
            msg.timestamp = 100;
            msgs.push_back(msg);
        }
        {
            RawDocumentParser::Message msg;
            string rawData = "userid=100;itemid=1;price=99;title=hello;CMD=add";
            msg.data = rawData;
            msg.timestamp = 100;
            msgs.push_back(msg);
        }
        ASSERT_TRUE(parser.parseMultiMessage(msgs, doc));
        ASSERT_EQ(1u, doc.GetMessageCount());
        ASSERT_EQ(1u, doc.GetDocumentCount());
    }
    {
        // all doc fail
        document::KVDocument doc;
        std::vector<RawDocumentParser::Message> msgs;
        {
            RawDocumentParser::Message msg;
            string rawData = "userid=100;itemid=1;CMD=add";
            msg.data = rawData;
            msg.timestamp = 100;
            msgs.push_back(msg);
        }
        {
            RawDocumentParser::Message msg;
            string rawData = "userid=100;itemid=1;CMD=add";
            msg.data = rawData;
            msg.timestamp = 100;
            msgs.push_back(msg);
        }
        ASSERT_FALSE(parser.parseMultiMessage(msgs, doc));
    }
}

}} // namespace indexlib::document
