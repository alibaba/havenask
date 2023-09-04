#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/document/document_parser/kv_parser/fb_kv_raw_document_parser.h"
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

class FbKvRawDocumentParserTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp();
    void CaseTearDown();
};

void FbKvRawDocumentParserTest::CaseSetUp() {}

void FbKvRawDocumentParserTest::CaseTearDown() {}

namespace {

std::string makeFbMessage(const std::map<std::string, std::string>& fields)
{
    flatbuffers::FlatBufferBuilder builder;

    std::vector<flatbuffers::Offset<document::proto::kv::Field>> fbFields;
    for (const auto& item : fields) {
        auto fieldName = builder.CreateString(item.first.c_str(), item.first.size());

        if (item.first == "nid") {
            // read from int value
            document::proto::kv::FieldBuilder fieldBuilder(builder);
            fieldBuilder.add_integer_value(autil::StringUtil::numberFromString<int64_t>(item.second));
            fieldBuilder.add_name(fieldName);
            auto field = fieldBuilder.Finish();
            fbFields.push_back(field);
        } else {
            auto fieldValue = builder.CreateString(item.second.c_str(), item.second.size());
            document::proto::kv::FieldBuilder fieldBuilder(builder);
            fieldBuilder.add_string_value(fieldValue);
            fieldBuilder.add_name(fieldName);
            auto field = fieldBuilder.Finish();
            fbFields.push_back(field);
        }
    }

    auto message =
        document::proto::kv::CreateDocMessage(builder, builder.CreateVector(fbFields.data(), fbFields.size()));
    builder.Finish(message);

    return std::string((char*)builder.GetBufferPointer(), builder.GetSize());
}

} // namespace

TEST_F(FbKvRawDocumentParserTest, testKKVFbMessage)
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
    "attributes": ["seller_id", "uid", "nid"],
    "fields": [
        { "multi_value": false, "field_type": "INT64", "field_name": "nid" },
        { "multi_value": false, "field_type": "INT64", "field_name": "uid" },
        { "multi_value": false, "field_type": "INT64", "field_name": "seller_id" },
        { "multi_value": false, "field_type": "STRING", "field_name": "ha_reserved_timestamp" },
        { "multi_value": false, "field_type": "UINT32", "field_name": "ttl" }
    ],
    "table_name": "click",
    "table_type": "kkv",
    "ttl_from_doc": true,
    "enable_ttl": true,
    "ttl_field_name": "ttl"
}
)";
    auto schema = std::make_shared<IndexPartitionSchema>();
    FromJsonString(*schema, schemaStr);

    FbKvRawDocumentParser parser(schema);
    ASSERT_TRUE(parser.construct(DocumentInitParamPtr()));

    document::KVDocument doc;

    //    string rawData = "nid=100;uid=1;seller_id=99;ha_reserved_timestamp=7;ttl=123";
    //
    RawDocumentParser::Message rawData;
    std::map<std::string, std::string> kvMap = {
        {"nid", "100"}, {"seller_id", "99"}, {"ha_reserved_timestamp", "7"}, {"ttl", "123"}, {"uid", "1"}};
    rawData.data = makeFbMessage(kvMap);
    rawData.timestamp = 9;

    ASSERT_TRUE(parser.parse(rawData, doc));

    ASSERT_EQ(1u, doc.GetMessageCount());
    ASSERT_EQ(1u, doc.GetDocumentCount());
    auto kvIndexDoc = doc.begin();

    ASSERT_EQ(100u, kvIndexDoc->GetPKeyHash());
    ASSERT_TRUE(kvIndexDoc->HasSKey());
    ASSERT_EQ(1u, kvIndexDoc->GetSKeyHash());
    ASSERT_EQ(7, kvIndexDoc->GetUserTimestamp());
    ASSERT_EQ(9, kvIndexDoc->GetTimestamp());
    ASSERT_EQ(123, kvIndexDoc->GetTTL());

    ASSERT_EQ(ADD_DOC, kvIndexDoc->GetDocOperateType());
    auto value = kvIndexDoc->GetValue();
    ASSERT_EQ(25, value.size());
    int64_t* addr = reinterpret_cast<int64_t*>((char*)value.data() + 9); // var len header and attr hash header
    ASSERT_EQ(99, *addr);
    ASSERT_EQ(100, *(addr + 1));

    ASSERT_EQ(7, doc.GetUserTimestamp());
}

}} // namespace indexlib::document
