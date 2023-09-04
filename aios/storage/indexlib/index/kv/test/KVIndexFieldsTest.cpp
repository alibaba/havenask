#include "indexlib/index/kv/KVIndexFields.h"

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/IndexConfigHash.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/KVIndexFieldsParser.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/util/PooledUniquePtr.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlibv2::document;

namespace indexlibv2::index {

class KVIndexFieldsTest : public TESTBASE
{
};

TEST_F(KVIndexFieldsTest, testSimple)
{
    autil::mem_pool::Pool pool;
    KVIndexFields kvFields1(&pool);
    // pkFieldName pkFieldValue value
    string value1 = "pk1";
    string value2 = "pk2";
    KVIndexFields::SingleField field1 {1, 11, true, value1, 10, 100, true, {}, {}};
    ASSERT_TRUE(kvFields1.AddSingleField(123, field1));
    KVIndexFields::SingleField field2 {2, 22, false, value2, 20, 200, false, {}, {}};
    ASSERT_TRUE(kvFields1.AddSingleField(456, field2));

    autil::DataBuffer dataBuffer;
    kvFields1.serialize(dataBuffer);

    KVIndexFields kvFields2(&pool);
    kvFields2.deserialize(dataBuffer);

    auto fieldA = *kvFields2.GetSingleField(123);
    auto fieldB = *kvFields2.GetSingleField(456);

    auto checkSingleField = [](const auto& fieldExpect, const auto& field) {
        ASSERT_EQ(fieldExpect.pkeyHash, field.pkeyHash);
        if (fieldExpect.hasSkey) {
            ASSERT_EQ(fieldExpect.skeyHash, field.skeyHash);
        } else {
            ASSERT_EQ(0, field.skeyHash);
        }
        ASSERT_EQ(fieldExpect.hasSkey, field.hasSkey);
        ASSERT_EQ(fieldExpect.value, field.value);
        ASSERT_EQ(fieldExpect.ttl, field.ttl);
        ASSERT_EQ(fieldExpect.userTimestamp, field.userTimestamp);
        ASSERT_EQ(fieldExpect.hasFormatError, field.hasFormatError);
        ASSERT_TRUE(field.pkFieldName.empty());
        ASSERT_TRUE(field.pkFieldValue.empty());
    };
    checkSingleField(field1, fieldA);
    checkSingleField(field2, fieldB);
}

TEST_F(KVIndexFieldsTest, testParser)
{
    auto kvIndexConfig = make_shared<config::KVIndexConfig>();
    auto jsonStr = R"(
    {
        "index_name" : "index1",
        "key_field"  : "key",
        "value_fields" : [
                "weights"
        ],
        "use_number_hash":true
    }
    )";
    auto any = autil::legacy::json::ParseJson(jsonStr);
    std::vector<std::shared_ptr<config::FieldConfig>> fieldConfigs;
    fieldConfigs.emplace_back(std::make_shared<config::FieldConfig>("key", ft_int64, false));
    fieldConfigs.emplace_back(std::make_shared<config::FieldConfig>("weights", ft_float, true));
    for (size_t i = 0; i < fieldConfigs.size(); ++i) {
        fieldConfigs[i]->SetFieldId(i);
    }
    config::MutableJson runtimeSettings;
    config::IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
    ASSERT_NO_THROW(kvIndexConfig->Deserialize(any, 0, resource));

    KVIndexFieldsParser parser;

    ASSERT_TRUE(parser.Init({kvIndexConfig}, nullptr).IsOK());
    ExtendDocument extDoc;
    auto rawDoc = std::shared_ptr<RawDocument>(RawDocumentMaker::Make("cmd=add,key=1,weights=2.2,ts=3").release());
    extDoc.setRawDocument(rawDoc);
    autil::mem_pool::Pool pool;
    auto indexFields = parser.Parse(extDoc, &pool);
    ASSERT_TRUE(indexFields);
    auto kvIndexFields = dynamic_cast<KVIndexFields*>(indexFields.get());
    ASSERT_TRUE(kvIndexFields);
    ASSERT_EQ(autil::StringView(index::KV_INDEX_TYPE_STR), kvIndexFields->GetIndexType());
    ASSERT_EQ(0, kvIndexFields->EstimateMemory());
    ASSERT_FALSE(kvIndexFields->HasFormatError());
    ASSERT_EQ(ADD_DOC, kvIndexFields->GetDocOperateType());
    auto hash = config::IndexConfigHash::Hash(kvIndexConfig);
    auto field = kvIndexFields->GetSingleField(hash);
    ASSERT_TRUE(field);
    ASSERT_EQ(1, field->pkeyHash);
    ASSERT_FALSE(field->hasSkey);
    ASSERT_EQ(0, field->skeyHash);
    ASSERT_FALSE(field->value.empty());
    ASSERT_EQ(UNINITIALIZED_TTL, field->ttl);
    ASSERT_EQ(3, field->userTimestamp);
    ASSERT_FALSE(field->hasFormatError);
    ASSERT_EQ(autil::StringView("key"), field->pkFieldName);
    ASSERT_EQ(autil::StringView("1"), field->pkFieldValue);
}

TEST_F(KVIndexFieldsTest, testDeserialize)
{
    autil::mem_pool::Pool pool;
    KVIndexFields kvFields1(&pool);
    // pkFieldName pkFieldValue value
    string value1 = "pk1";
    string value2 = "pk2";
    KVIndexFields::SingleField field1 {1, 11, true, value1, 10, 100, true, {}, {}};
    ASSERT_TRUE(kvFields1.AddSingleField(123, field1));
    KVIndexFields::SingleField field2 {2, 22, false, value2, 20, 200, false, {}, {}};
    ASSERT_TRUE(kvFields1.AddSingleField(456, field2));

    autil::DataBuffer dataBuffer;
    kvFields1.serialize(dataBuffer);

    KVIndexFieldsParser parser;
    ASSERT_TRUE(parser.Init({}, nullptr).IsOK());
    auto kvFieldsPtr = parser.Parse(autil::StringView(dataBuffer.getData(), dataBuffer.getDataLen()), &pool);
    ASSERT_TRUE(kvFieldsPtr);
    auto typedFields = dynamic_cast<KVIndexFields*>(kvFieldsPtr.get());
    ASSERT_TRUE(typedFields);
    auto& kvFields2 = *typedFields;

    auto fieldA = *kvFields2.GetSingleField(123);
    auto fieldB = *kvFields2.GetSingleField(456);

    auto checkSingleField = [](const auto& fieldExpect, const auto& field) {
        ASSERT_EQ(fieldExpect.pkeyHash, field.pkeyHash);
        if (fieldExpect.hasSkey) {
            ASSERT_EQ(fieldExpect.skeyHash, field.skeyHash);
        } else {
            ASSERT_EQ(0, field.skeyHash);
        }
        ASSERT_EQ(fieldExpect.hasSkey, field.hasSkey);
        ASSERT_EQ(fieldExpect.value, field.value);
        ASSERT_EQ(fieldExpect.ttl, field.ttl);
        ASSERT_EQ(fieldExpect.userTimestamp, field.userTimestamp);
        ASSERT_EQ(fieldExpect.hasFormatError, field.hasFormatError);
        ASSERT_TRUE(field.pkFieldName.empty());
        ASSERT_TRUE(field.pkFieldValue.empty());
    };
    checkSingleField(field1, fieldA);
    checkSingleField(field2, fieldB);
}

} // namespace indexlibv2::index
