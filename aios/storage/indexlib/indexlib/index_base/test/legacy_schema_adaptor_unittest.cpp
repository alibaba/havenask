#include "indexlib/common_define.h"
#include "indexlib/config/disable_fields_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index/ann/Common.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace index_base {

class LegacySchemaAdaptorTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}

    void TestSimple();
    void TestDisableFields();
};
INDEXLIB_UNIT_TEST_CASE(LegacySchemaAdaptorTest, TestSimple);
INDEXLIB_UNIT_TEST_CASE(LegacySchemaAdaptorTest, TestDisableFields);

//////////////////////////////////////////////////////////////////////
void LegacySchemaAdaptorTest::TestSimple()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "pkstr:string;text1:text;long1:uint32;long2:int32;long3:int64;ulong4:uint64;multi_long:uint32:true;"
        "updatable_multi_long:uint32:true:true;field4:location",
        "pk:primarykey64:pkstr;pack1:pack:text1;idx2:number:long1;idx3:text:text1;idx4:spatial:field4;aitheta_indexer:"
        "customized:ulong4",
        "long1;multi_long;updatable_multi_long;pack_attr1:long2,long3;pack_attr2:ulong4", "pkstr;");
    IndexPartitionOptions options;
    ASSERT_THROW(SchemaRewriter::Rewrite(schema, options, GET_PARTITION_DIRECTORY()), IndexCollapsedException);
    auto tabletSchema = std::make_shared<indexlib::config::LegacySchemaAdapter>(schema);

    // attributes
    auto attributeConfigs = tabletSchema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    std::vector<std::string> attributeNames;
    for (const auto& attributeConfig : attributeConfigs) {
        attributeNames.push_back(attributeConfig->GetIndexName());
    }
    std::vector<std::string> expectAttributeNames = {"long1", "multi_long", "updatable_multi_long", "field4"};
    ASSERT_EQ(expectAttributeNames, attributeNames);
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, "multi_long"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, "field4"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, "long1"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, "updatable_multi_long"));
    // pack_attribute
    auto packAttributeConfigs = tabletSchema->GetIndexConfigs(index::PACK_ATTRIBUTE_INDEX_TYPE_STR);
    std::vector<std::string> packAttributeNames;
    for (const auto& packAttributeConfig : packAttributeConfigs) {
        packAttributeNames.push_back(packAttributeConfig->GetIndexName());
    }
    std::vector<std::string> expectPackAttributeNames = {"pack_attr1", "pack_attr2"};
    ASSERT_EQ(expectPackAttributeNames, packAttributeNames);
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::PACK_ATTRIBUTE_INDEX_TYPE_STR, "pack_attr1"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::PACK_ATTRIBUTE_INDEX_TYPE_STR, "pack_attr2"));
    // general_value
    auto generalValueConfigs = tabletSchema->GetIndexConfigs(index::GENERAL_VALUE_INDEX_TYPE_STR);
    std::vector<std::string> generalValueNames;
    for (const auto& generalValueConfig : generalValueConfigs) {
        generalValueNames.push_back(generalValueConfig->GetIndexName());
    }
    std::vector<std::string> expectGeneralValueNames = {
        "long1", "multi_long", "updatable_multi_long", "long2", "long3", "ulong4", "field4"};
    ASSERT_EQ(expectGeneralValueNames, generalValueNames);
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "multi_long"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "field4"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "long1"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "long2"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "long3"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "ulong4"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "updatable_multi_long"));
    // inverted
    auto invertedConfigs = tabletSchema->GetIndexConfigs(index::INVERTED_INDEX_TYPE_STR);
    std::vector<std::string> invertedNames;
    for (const auto& invertedConfig : invertedConfigs) {
        invertedNames.push_back(invertedConfig->GetIndexName());
    }
    std::vector<std::string> expectInvertedNames = {"pack1", "idx2", "idx3", "idx4"};
    ASSERT_EQ(expectInvertedNames, invertedNames);
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::INVERTED_INDEX_TYPE_STR, "pack1"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::INVERTED_INDEX_TYPE_STR, "pk"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::INVERTED_INDEX_TYPE_STR, "idx2"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::INVERTED_INDEX_TYPE_STR, "idx3"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::INVERTED_INDEX_TYPE_STR, "idx4"));
    // ann
    auto annConfigs = tabletSchema->GetIndexConfigs(indexlibv2::index::ANN_INDEX_TYPE_STR);
    ASSERT_EQ(1, annConfigs.size());
    ASSERT_EQ("aitheta_indexer", annConfigs[0]->GetIndexName());
    ASSERT_TRUE(tabletSchema->GetIndexConfig(indexlibv2::index::ANN_INDEX_TYPE_STR, "aitheta_indexer"));
    // pk
    auto pkConfigs = tabletSchema->GetIndexConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR);
    ASSERT_EQ(1, pkConfigs.size());
    ASSERT_EQ("pk", pkConfigs[0]->GetIndexName());
    ASSERT_TRUE(tabletSchema->GetIndexConfig(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR, "pk"));
    // general_inverted
    auto generalInvertedConfigs = tabletSchema->GetIndexConfigs(index::GENERAL_INVERTED_INDEX_TYPE_STR);
    std::vector<std::string> generalInvertedNames;
    for (const auto& generalInvertedConfig : generalInvertedConfigs) {
        generalInvertedNames.push_back(generalInvertedConfig->GetIndexName());
    }
    std::vector<std::string> expectGeneralInvertedNames = {"pk", "pack1", "idx2", "idx3", "idx4", "aitheta_indexer"};
    ASSERT_EQ(expectGeneralInvertedNames, generalInvertedNames);
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_INVERTED_INDEX_TYPE_STR, "pack1"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_INVERTED_INDEX_TYPE_STR, "pk"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_INVERTED_INDEX_TYPE_STR, "idx2"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_INVERTED_INDEX_TYPE_STR, "idx3"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_INVERTED_INDEX_TYPE_STR, "idx4"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_INVERTED_INDEX_TYPE_STR, "aitheta_indexer"));
    // summary
    auto summaryConfigs = tabletSchema->GetIndexConfigs(index::SUMMARY_INDEX_TYPE_STR);
    ASSERT_EQ(1, summaryConfigs.size());
    ASSERT_EQ("summary", summaryConfigs[0]->GetIndexName());
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::SUMMARY_INDEX_TYPE_STR, "summary"));
}

void LegacySchemaAdaptorTest::TestDisableFields()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "pkstr:string;text1:text;long1:uint32;long2:int32;long3:int64;ulong4:uint64;multi_long:uint32:true;"
        "updatable_multi_long:uint32:true:true;field4:location",
        "pk:primarykey64:pkstr;pack1:pack:text1;idx2:number:long1;idx3:text:text1;idx4:spatial:field4;aitheta_indexer:"
        "customized:ulong4",
        "long1;multi_long;updatable_multi_long;pack_attr1:long2,long3;pack_attr2:ulong4", "pkstr;");
    IndexPartitionOptions options;
    options.GetOnlineConfig().GetDisableFieldsConfig().attributes = {"long1", "updatable_multi_long"};
    options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes = {"pack_attr1", "sub_pack_attr2"};
    options.GetOnlineConfig().GetDisableFieldsConfig().indexes = {"idx2", "idx3", "idx4"};
    options.GetOnlineConfig().GetDisableFieldsConfig().summarys = DisableFieldsConfig::SDF_FIELD_ALL;
    ASSERT_THROW(SchemaRewriter::Rewrite(schema, options, GET_PARTITION_DIRECTORY()), IndexCollapsedException);
    auto tabletSchema = std::make_shared<indexlib::config::LegacySchemaAdapter>(schema);

    // auto indexConfigs = tabletSchema->GetIndexConfigs();
    // std::vector<std::string> indexNames;
    // for (const auto& indexConfig: indexConfigs) {
    //     indexNames.push_back(indexConfig->GetIndexName());
    // }
    // std::vector<std::string> expectIndexNames = {"multi_long", "field4", "pkstr"};
    // ASSERT_EQ(expectIndexNames, indexNames);

    // attributes
    auto attributeConfigs = tabletSchema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    std::vector<std::string> attributeNames;
    for (const auto& attributeConfig : attributeConfigs) {
        attributeNames.push_back(attributeConfig->GetIndexName());
    }
    std::vector<std::string> expectAttributeNames = {"multi_long", "field4"};
    ASSERT_EQ(expectAttributeNames, attributeNames);
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, "multi_long"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, "field4"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, "long1"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, "updatable_multi_long"));
    // pack_attribute
    auto packAttributeConfigs = tabletSchema->GetIndexConfigs(index::PACK_ATTRIBUTE_INDEX_TYPE_STR);
    std::vector<std::string> packAttributeNames;
    for (const auto& packAttributeConfig : packAttributeConfigs) {
        packAttributeNames.push_back(packAttributeConfig->GetIndexName());
    }
    std::vector<std::string> expectPackAttributeNames = {"pack_attr2"};
    ASSERT_EQ(expectPackAttributeNames, packAttributeNames);
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::PACK_ATTRIBUTE_INDEX_TYPE_STR, "pack_attr1"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::PACK_ATTRIBUTE_INDEX_TYPE_STR, "pack_attr2"));
    // general_value
    auto generalValueConfigs = tabletSchema->GetIndexConfigs(index::GENERAL_VALUE_INDEX_TYPE_STR);
    std::vector<std::string> generalValueNames;
    for (const auto& generalValueConfig : generalValueConfigs) {
        generalValueNames.push_back(generalValueConfig->GetIndexName());
    }
    std::vector<std::string> expectGeneralValueNames = {"multi_long", "ulong4", "field4"};
    ASSERT_EQ(expectGeneralValueNames, generalValueNames);
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "multi_long"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "field4"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "long1"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "long2"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "long3"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "ulong4"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::GENERAL_VALUE_INDEX_TYPE_STR, "updatable_multi_long"));
    // inverted
    auto invertedConfigs = tabletSchema->GetIndexConfigs(index::INVERTED_INDEX_TYPE_STR);
    std::vector<std::string> invertedNames;
    for (const auto& invertedConfig : invertedConfigs) {
        invertedNames.push_back(invertedConfig->GetIndexName());
    }
    std::vector<std::string> expectInvertedNames = {"pack1"};
    ASSERT_EQ(expectInvertedNames, invertedNames);
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::INVERTED_INDEX_TYPE_STR, "pack1"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::INVERTED_INDEX_TYPE_STR, "pk"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::INVERTED_INDEX_TYPE_STR, "idx2"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::INVERTED_INDEX_TYPE_STR, "idx3"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::INVERTED_INDEX_TYPE_STR, "idx4"));
    // ann
    auto annConfigs = tabletSchema->GetIndexConfigs(indexlibv2::index::ANN_INDEX_TYPE_STR);
    ASSERT_EQ(1, annConfigs.size());
    ASSERT_EQ("aitheta_indexer", annConfigs[0]->GetIndexName());
    ASSERT_TRUE(tabletSchema->GetIndexConfig(indexlibv2::index::ANN_INDEX_TYPE_STR, "aitheta_indexer"));
    // pk
    auto pkConfigs = tabletSchema->GetIndexConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR);
    ASSERT_EQ(1, pkConfigs.size());
    ASSERT_EQ("pk", pkConfigs[0]->GetIndexName());
    ASSERT_TRUE(tabletSchema->GetIndexConfig(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR, "pk"));
    // general_inverted
    auto generalInvertedConfigs = tabletSchema->GetIndexConfigs(index::GENERAL_INVERTED_INDEX_TYPE_STR);
    std::vector<std::string> generalInvertedNames;
    for (const auto& generalInvertedConfig : generalInvertedConfigs) {
        generalInvertedNames.push_back(generalInvertedConfig->GetIndexName());
    }
    std::vector<std::string> expectGeneralInvertedNames = {"pk", "pack1", "aitheta_indexer"};
    ASSERT_EQ(expectGeneralInvertedNames, generalInvertedNames);
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_INVERTED_INDEX_TYPE_STR, "pack1"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_INVERTED_INDEX_TYPE_STR, "pk"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::GENERAL_INVERTED_INDEX_TYPE_STR, "idx2"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::GENERAL_INVERTED_INDEX_TYPE_STR, "idx3"));
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::GENERAL_INVERTED_INDEX_TYPE_STR, "idx4"));
    ASSERT_TRUE(tabletSchema->GetIndexConfig(index::GENERAL_INVERTED_INDEX_TYPE_STR, "aitheta_indexer"));
    // summary
    auto summaryConfigs = tabletSchema->GetIndexConfigs(index::SUMMARY_INDEX_TYPE_STR);
    ASSERT_TRUE(summaryConfigs.empty());
    ASSERT_FALSE(tabletSchema->GetIndexConfig(index::SUMMARY_INDEX_TYPE_STR, "summary"));
}

}} // namespace indexlib::index_base
