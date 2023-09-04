#include "build_service/util/SourceFieldExtractorUtil.h"

#include "indexlib/base/Constant.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "unittest/unittest.h"

namespace build_service::util {

class SourceFieldExtractorUtilTest : public TESTBASE
{
public:
    SourceFieldExtractorUtilTest() = default;
    ~SourceFieldExtractorUtilTest() = default;
    void setUp() override {};
    void tearDown() override {};

private:
    std::shared_ptr<indexlibv2::config::ITabletSchema> getDefaultSchema() const;
};

std::shared_ptr<indexlibv2::config::ITabletSchema> SourceFieldExtractorUtilTest::getDefaultSchema() const
{
    std::string schemaStr = R"({
        "attributes": [
            "key1",
            "key2",
            "attributes",
            "attributes1",
            "virtual_timestamp_field_name",
            "virtual_hash_id_field_name",
            "attributes_key3",
            "attributes_key4",
            "attributes_key5"
        ],
        "fields": [
            {
                "field_name": "virtual_timestamp_field_name",
                "field_type": "INT64"
            },
            {
                "field_name": "virtual_hash_id_field_name",
                "field_type": "UINT16"
            },
            {
                "field_name": "key1",
                "field_type": "STRING"
            },
            {
                "field_name": "key2",
                "field_type": "STRING"
            },
            {
                "field_name": "attributes_key3",
                "field_type": "STRING",
                "user_defined_param":{
                    "source_field":{
                        "field_name" : "attributes",
                        "key" : "key3",
                        "kv_pair_separator" : "+",
                        "kv_separator" : ":"
                     }
                }         
            },
            {
                "field_name": "attributes",
                "field_type": "STRING"
            },
            {
                "field_name": "attributes1",
                "field_type": "STRING"
            },
            {
                "field_name": "attributes_key4",
                "field_type": "STRING",
                "user_defined_param":{
                    "source_field":{
                        "field_name" : "attributes1",
                        "key" : "key4",
                        "kv_pair_separator" : "+",
                        "kv_separator" : "-"
                     }
                }         
            },
            {
                "field_name": "attributes_key5",
                "field_type": "STRING",
                "user_defined_param":{
                    "source_field":{
                        "field_name" : "attributes",
                        "key" : "key5",
                        "kv_pair_separator" : "+",
                        "kv_separator" : ":",
                        "tokenize_separator" : " "
                     }
                }         
            }
        ],
        "indexs": [
            {
                "has_primary_key_attribute": true,
                "index_fields": "key1",
                "index_name": "pk",
                "index_type": "PRIMARYKEY64"
            }
        ],
        "table_name": "noname"
    })";
    return indexlibv2::framework::TabletSchemaLoader::LoadSchema(schemaStr);
}

TEST_F(SourceFieldExtractorUtilTest, testNeedExtractSourceField)
{
    {
        auto fieldConfig = getDefaultSchema()->GetFieldConfig("attributes");
        std::map<std::string, autil::legacy::Any> userDefinedParam;
        fieldConfig->SetUserDefinedParam(userDefinedParam);
        auto ret = SourceFieldExtractorUtil::needExtractSourceField(fieldConfig);
        ASSERT_EQ(ret, false);
    }
    {
        auto fieldConfig = getDefaultSchema()->GetFieldConfig("attributes");
        std::map<std::string, autil::legacy::Any> userDefinedParam;
        userDefinedParam["source"] = autil::legacy::Any(std::string("source_value"));
        fieldConfig->SetUserDefinedParam(userDefinedParam);
        auto ret = SourceFieldExtractorUtil::needExtractSourceField(fieldConfig);
        ASSERT_EQ(ret, false);
    }
    {
        auto fieldConfig = getDefaultSchema()->GetFieldConfig("attributes");
        std::map<std::string, autil::legacy::Any> userDefinedParam;
        userDefinedParam[SourceFieldExtractorUtil::SOURCE_FIELD] =
            autil::legacy::Any(std::string("source_field_value"));
        fieldConfig->SetUserDefinedParam(userDefinedParam);
        auto ret = SourceFieldExtractorUtil::needExtractSourceField(fieldConfig);
        ASSERT_EQ(ret, true);
    }
}

TEST_F(SourceFieldExtractorUtilTest, testGetValue4KvMap)
{
    util::UnorderedStringViewKVMap kvMap;

    const std::string attributes = "key1:val1;key2:val2";
    SourceFieldExtractorUtil::SourceFieldParam param;
    param.kvPairSeparator = ";";
    param.kvSeparator = ":";
    param.key = "key";
    param.fieldName = "test_kvmap";

    SourceFieldParsedInfo parsedInfo;
    parsedInfo.first = attributes;
    parsedInfo.second = kvMap;
    std::string value;
    ASSERT_TRUE(SourceFieldExtractorUtil::parseRawString(param, parsedInfo));
    // key not exist in attributes
    ASSERT_FALSE(SourceFieldExtractorUtil::getValue(parsedInfo, param, value));
    ASSERT_TRUE(value.empty());

    param.key = "key1";
    ASSERT_TRUE(SourceFieldExtractorUtil::getValue(parsedInfo, param, value));
    ASSERT_EQ("val1", value);
}

TEST_F(SourceFieldExtractorUtilTest, testGetValue4ConcatAllKeys)
{
    util::UnorderedStringViewKVMap kvMap;

    const std::string attributes = "key1:val1;key2:val2";
    SourceFieldExtractorUtil::SourceFieldParam param;
    param.kvPairSeparator = ";";
    param.kvSeparator = ":";
    param.fieldName = "test_kvmap";
    param.tokenizeSeparator = ' ';

    SourceFieldParsedInfo parsedInfo;
    parsedInfo.first = attributes;
    parsedInfo.second = kvMap;
    std::string value;
    ASSERT_TRUE(SourceFieldExtractorUtil::parseRawString(param, parsedInfo));
    ASSERT_TRUE(SourceFieldExtractorUtil::getValue(parsedInfo, param, value));
    ASSERT_EQ("key2 key1", value);
}

TEST_F(SourceFieldExtractorUtilTest, testParseRawString4KvMap)
{
    SourceFieldExtractorUtil::SourceFieldParam param;
    param.kvPairSeparator = ";";
    param.kvSeparator = ":";
    param.key = "kvmap";
    param.fieldName = "test_kvmap";

    SourceFieldParsedInfo parsedInfo;
    {
        // too many split items
        const std::string rawStr = "key1:val1;key2:val2:val3";
        parsedInfo.first = rawStr;
        ASSERT_FALSE(SourceFieldExtractorUtil::parseRawString(param, parsedInfo));
        const auto& kvMap = parsedInfo.second;
        ASSERT_EQ(1, kvMap.size());
        ASSERT_THAT(kvMap, testing::UnorderedElementsAre(std::make_pair("key1", "val1")));
    }

    {
        // parseSuccess
        parsedInfo.first = "key1:val1;key2:val2";
        ASSERT_TRUE(SourceFieldExtractorUtil::parseRawString(param, parsedInfo));
        const auto& kvMap = parsedInfo.second;
        ASSERT_EQ(2, kvMap.size());
        ASSERT_THAT(kvMap,
                    testing::UnorderedElementsAre(std::make_pair("key1", "val1"), std::make_pair("key2", "val2")));
    }
}

// TEST_F(SourceFieldExtractorUtilTest, testParseRawString4ConcatAllKeys)
// {
//     SourceFieldExtractorUtil::SourceFieldParam param;
//     param.kvPairSeparator = ";";
//     param.kvSeparator = ":";
//     param.fieldName = "test_concat_all_keys";
//     param.tokenizeSeparator = indexlib::MULTI_VALUE_SEPARATOR;

//     SourceFieldParsedInfo parsedInfo;
//     {
//         // too many split items
//         const std::string rawStr = "key1:val1;key2:val2:val3";
//         parsedInfo.first = rawStr;
//         ASSERT_FALSE(SourceFieldExtractorUtil::parseRawString4ConcatAllKeys(param, parsedInfo));
//         ASSERT_TRUE(parsedInfo.second.empty());
//     }

//     {
//         // parseSuccess with default tokenize-separator
//         const std::string rawStr = "key1:val1;key2:val2";
//         parsedInfo.first = rawStr;
//         ASSERT_TRUE(SourceFieldExtractorUtil::parseRawString4ConcatAllKeys(param, parsedInfo));
//         const std::string expectedStr = std::string("key1") + indexlib::MULTI_VALUE_SEPARATOR + "key2";
//         ASSERT_EQ(expectedStr, parsedInfo.first);
//         ASSERT_TRUE(parsedInfo.second.empty());
//     }

//     {
//         // parseSuccess with customize tokenize-separator
//         const std::string separator = " ";
//         const std::string rawStr = "key1:val1;key2:val2";
//         param.tokenizeSeparator = separator;
//         parsedInfo.first = rawStr;
//         ASSERT_TRUE(SourceFieldExtractorUtil::parseRawString4ConcatAllKeys(param, parsedInfo));
//         const std::string expectedStr = std::string("key1") + separator + "key2";
//         ASSERT_EQ(expectedStr, parsedInfo.first);
//         ASSERT_TRUE(parsedInfo.second.empty());
//     }
// }

TEST_F(SourceFieldExtractorUtilTest, testParseRawString1)
{
    SourceFieldExtractorUtil::SourceFieldParam param;
    param.kvPairSeparator = ";";
    param.kvSeparator = ":";
    param.key = "key";
    param.fieldName = "ut_test";

    const std::string rawStr = "key1:val1;key2:val2";

    SourceFieldParsedInfo parsedInfo;
    parsedInfo.first = rawStr;

    // test kv map
    ASSERT_TRUE(SourceFieldExtractorUtil::parseRawString(param, parsedInfo));
    const auto& kvMap = parsedInfo.second;
    ASSERT_EQ(2, kvMap.size());
    ASSERT_THAT(parsedInfo.second,
                testing::UnorderedElementsAre(std::make_pair("key1", "val1"), std::make_pair("key2", "val2")));

    const std::string separator = " ";
    param.key.clear();
    param.tokenizeSeparator = separator; // custom separator
    parsedInfo.second.clear();
    ASSERT_TRUE(SourceFieldExtractorUtil::parseRawString(param, parsedInfo));
    ASSERT_EQ(2, parsedInfo.second.size());
    ASSERT_THAT(parsedInfo.second,
                testing::UnorderedElementsAre(std::make_pair("key1", "val1"), std::make_pair("key2", "val2")));
}

TEST_F(SourceFieldExtractorUtilTest, testCheckUnifiedSeparator)
{
    SourceField2Separators field2Separators;
    const std::string kvSep = ":";
    const std::string kvPairSep = ";";
    const std::string fieldName = "attributes";
    std::string tokenizeSeparator;
    tokenizeSeparator += indexlib::MULTI_VALUE_SEPARATOR;
    SourceFieldExtractorUtil::SourceFieldParam param {kvPairSep, kvSep, tokenizeSeparator, fieldName,
                                                      /*key*/ ""};
    // success insert one item
    ASSERT_TRUE(SourceFieldExtractorUtil::checkUnifiedSeparator(param, field2Separators));
    ASSERT_EQ(1, field2Separators.size());
    ASSERT_EQ(fieldName, field2Separators.begin()->first);
    ASSERT_EQ(std::make_pair(kvPairSep, kvSep), field2Separators.begin()->second);

    // exist source field
    auto existParam = param;
    ASSERT_TRUE(SourceFieldExtractorUtil::checkUnifiedSeparator(existParam, field2Separators));
    ASSERT_EQ(1, field2Separators.size());
    ASSERT_EQ(fieldName, field2Separators.begin()->first);
    ASSERT_EQ(std::make_pair(kvPairSep, kvSep), field2Separators.begin()->second);

    // mismatch separator
    auto mismatchParam = param;
    mismatchParam.kvSeparator = ",";
    ASSERT_FALSE(SourceFieldExtractorUtil::checkUnifiedSeparator(mismatchParam, field2Separators));
    ASSERT_EQ(1, field2Separators.size());
    ASSERT_EQ(fieldName, field2Separators.begin()->first);
    ASSERT_EQ(std::make_pair(kvPairSep, kvSep), field2Separators.begin()->second);

    mismatchParam = param;
    mismatchParam.kvPairSeparator = ",";
    ASSERT_FALSE(SourceFieldExtractorUtil::checkUnifiedSeparator(mismatchParam, field2Separators));
    ASSERT_EQ(1, field2Separators.size());
    ASSERT_EQ(fieldName, field2Separators.begin()->first);
    ASSERT_EQ(std::make_pair(kvPairSep, kvSep), field2Separators.begin()->second);
}

TEST_F(SourceFieldExtractorUtilTest, testGetSourceFieldParam4KvMap)
{
    const std::string fieldName = "attributes";
    const std::string key = "demo";
    const std::string kvPairSeparator = ";";
    const std::string kvSeparator = ":";
    SourceFieldExtractorUtil::SourceFieldParam param {kvPairSeparator, kvSeparator, /*tokenizeSeparator*/ "", fieldName,
                                                      key};

    auto fieldConfig = getDefaultSchema()->GetFieldConfig(fieldName);
    autil::legacy::json::JsonMap kvMap;
    kvMap[util::SourceFieldExtractorUtil::FIELD_NAME] = autil::legacy::Any(fieldName);
    kvMap[util::SourceFieldExtractorUtil::KEY] = autil::legacy::Any(std::string(key));
    kvMap[util::SourceFieldExtractorUtil::KV_PAIR_SEPARATOR] = autil::legacy::Any(kvPairSeparator);
    kvMap[util::SourceFieldExtractorUtil::KV_SEPARATOR] = autil::legacy::Any(kvSeparator);

    std::map<std::string, autil::legacy::Any> userDefinedParam;
    userDefinedParam[util::SourceFieldExtractorUtil::SOURCE_FIELD] = autil::legacy::Any(kvMap);
    fieldConfig->SetUserDefinedParam(userDefinedParam);

    SourceFieldExtractorUtil::SourceFieldParam retParam;
    ASSERT_TRUE(SourceFieldExtractorUtil::getSourceFieldParam(fieldConfig, retParam));
    ASSERT_EQ(param, retParam);
}

TEST_F(SourceFieldExtractorUtilTest, testGetSourceFieldParam4ConcatAllKeys)
{
    const std::string fieldName = "attributes";
    const std::string kvPairSeparator = ";";
    const std::string kvSeparator = ":";
    std::string tokenizeSeparator;
    tokenizeSeparator += indexlib::MULTI_VALUE_SEPARATOR;
    SourceFieldExtractorUtil::SourceFieldParam param {kvPairSeparator, kvSeparator, tokenizeSeparator, fieldName,
                                                      /*key*/ ""};

    auto fieldConfig = getDefaultSchema()->GetFieldConfig(fieldName);
    autil::legacy::json::JsonMap kvMap;
    kvMap[util::SourceFieldExtractorUtil::FIELD_NAME] = autil::legacy::Any(fieldName);
    kvMap[util::SourceFieldExtractorUtil::KV_PAIR_SEPARATOR] = autil::legacy::Any(kvPairSeparator);
    kvMap[util::SourceFieldExtractorUtil::KV_SEPARATOR] = autil::legacy::Any(kvSeparator);
    kvMap[util::SourceFieldExtractorUtil::TOKENIZE_SEPARATOR] = autil::legacy::Any(tokenizeSeparator);

    std::map<std::string, autil::legacy::Any> userDefinedParam;
    userDefinedParam[util::SourceFieldExtractorUtil::SOURCE_FIELD] = autil::legacy::Any(kvMap);
    fieldConfig->SetUserDefinedParam(userDefinedParam);

    SourceFieldExtractorUtil::SourceFieldParam retParam;
    ASSERT_TRUE(SourceFieldExtractorUtil::getSourceFieldParam(fieldConfig, retParam));
    ASSERT_EQ(param, retParam);
}

} // namespace build_service::util
