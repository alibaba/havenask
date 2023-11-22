#include "indexlib/index_base/test/schema_rewriter_unittest.h"

#include "indexlib/config/disable_fields_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/test/modify_schema_maker.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SchemaRewriterTest);

SchemaRewriterTest::SchemaRewriterTest() {}

SchemaRewriterTest::~SchemaRewriterTest() {}

void SchemaRewriterTest::CaseSetUp() {}

void SchemaRewriterTest::CaseTearDown() {}

void SchemaRewriterTest::TestDisableFields()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "pkstr:string;text1:text;long1:uint32;long2:int32;long3:int64;multi_long:uint32:true;updatable_multi_long:"
        "uint32:true:true;field4:location",
        "pk:primarykey64:pkstr;pack1:pack:text1;idx2:number:long1;idx3:text:text1;idx4:spatial:field4",
        "long1;multi_long;updatable_multi_long;pack_attr1:long2,long3", "pkstr;");
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
        "substr1:string;subpkstr:string;sub_long:uint32;sub_long2:int32;sub_str2:string;sub_long3:int64;sub_str3:"
        "string",
        "subindex1:string:substr1;sub_pk:primarykey64:subpkstr",
        "substr1;subpkstr;sub_long;sub_pack_attr2:sub_long2,sub_str2;sub_pack_attr3:sub_long3,sub_str3", "");
    schema->SetSubIndexPartitionSchema(subSchema);

    IndexPartitionOptions options;
    options.GetOnlineConfig().GetDisableFieldsConfig().attributes = {"long1", "updatable_multi_long"};
    options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes = {"pack_attr1", "sub_pack_attr2"};
    options.GetOnlineConfig().GetDisableFieldsConfig().indexes = {"idx2", "idx3", "idx4"};
    ASSERT_THROW(SchemaRewriter::Rewrite(schema, options, GET_PARTITION_DIRECTORY()), IndexCollapsedException);

    const auto& attrSchema = schema->GetAttributeSchema();
    EXPECT_FALSE(attrSchema->GetAttributeConfig("multi_long")->IsDisabled());
    EXPECT_TRUE(attrSchema->GetAttributeConfig("long1")->IsDisabled());
    EXPECT_TRUE(attrSchema->GetAttributeConfig("updatable_multi_long")->IsDisabled());
    EXPECT_TRUE(attrSchema->GetAttributeConfig("long2")->IsDisabled());
    EXPECT_TRUE(attrSchema->GetAttributeConfig("long2")->GetPackAttributeConfig()->IsDisabled());
    EXPECT_TRUE(attrSchema->GetAttributeConfig("long3")->IsDisabled());
    EXPECT_TRUE(attrSchema->GetAttributeConfig("long3")->GetPackAttributeConfig()->IsDisabled());
    EXPECT_TRUE(attrSchema->GetPackAttributeConfig("pack_attr1")->IsDisabled());
    for (const auto& config : attrSchema->GetPackAttributeConfig("pack_attr1")->GetAttributeConfigVec()) {
        EXPECT_TRUE(config->IsDisabled());
    }

    const auto& subAttrSchema = subSchema->GetAttributeSchema();
    EXPECT_TRUE(subAttrSchema->GetAttributeConfig("sub_long2")->IsDisabled());
    EXPECT_TRUE(subAttrSchema->GetAttributeConfig("sub_long2")->GetPackAttributeConfig()->IsDisabled());
    EXPECT_TRUE(subAttrSchema->GetAttributeConfig("sub_str2")->IsDisabled());
    EXPECT_TRUE(subAttrSchema->GetAttributeConfig("sub_str2")->GetPackAttributeConfig()->IsDisabled());
    EXPECT_TRUE(subAttrSchema->GetPackAttributeConfig("sub_pack_attr2")->IsDisabled());
    for (const auto& config : subAttrSchema->GetPackAttributeConfig("sub_pack_attr2")->GetAttributeConfigVec()) {
        EXPECT_TRUE(config->IsDisabled());
    }

    EXPECT_FALSE(subAttrSchema->GetAttributeConfig("sub_long3")->IsDisabled());
    EXPECT_FALSE(subAttrSchema->GetAttributeConfig("sub_long3")->GetPackAttributeConfig()->IsDisabled());
    EXPECT_FALSE(subAttrSchema->GetAttributeConfig("sub_str3")->IsDisabled());
    EXPECT_FALSE(subAttrSchema->GetAttributeConfig("sub_str3")->GetPackAttributeConfig()->IsDisabled());
    EXPECT_FALSE(subAttrSchema->GetPackAttributeConfig("sub_pack_attr3")->IsDisabled());
    for (const auto& config : subAttrSchema->GetPackAttributeConfig("sub_pack_attr3")->GetAttributeConfigVec()) {
        EXPECT_FALSE(config->IsDisabled());
    }

    const auto& indexSchema = schema->GetIndexSchema();
    EXPECT_TRUE(indexSchema->GetIndexConfig("pk"));
    EXPECT_TRUE(indexSchema->GetIndexConfig("pack1"));
    EXPECT_TRUE(indexSchema->GetIndexConfig("idx2")->IsDisabled());
    EXPECT_TRUE(indexSchema->GetIndexConfig("idx3")->IsDisabled());
    EXPECT_TRUE(indexSchema->GetIndexConfig("idx4")->IsDisabled());
    EXPECT_TRUE(schema->GetSummarySchema());
}

void SchemaRewriterTest::TestRewriteWithModifyOperation()
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("pkstr:string;text1:text;long1:uint32;long2:int32;long3:int64;multi_long:uint32:true;"
                                "updatable_multi_long:uint32:true:true;",
                                "pk:primarykey64:pkstr;pack1:pack:text1;",
                                "long1;multi_long;updatable_multi_long;pack_attr1:long2,long3", "pkstr;");

    ModifySchemaMaker::AddModifyOperations(schema, "", "new_field:uint64;new_field2:uint16", "",
                                           "new_field;new_field2");

    IndexPartitionSchemaPtr cloneSchema(schema->Clone());
    IndexPartitionOptions options;
    ASSERT_THROW(SchemaRewriter::Rewrite(cloneSchema, options, GET_PARTITION_DIRECTORY()), IndexCollapsedException);
    ASSERT_NO_THROW(SchemaRewriter::RewriteForRealtimeTimestamp(cloneSchema));

    ASSERT_TRUE(cloneSchema->GetIndexSchema()->GetIndexConfig("virtual_timestamp_index"));
}

void SchemaRewriterTest::TestDisableAttributeInSummary()
{
    IndexPartitionSchemaPtr baseSchema =
        SchemaMaker::MakeSchema("pkstr:string;long1:uint32;long2:int32;long3:int64", "pk:primarykey64:pkstr;",
                                "long1;pack_attr1:long2,long3", "pkstr;long1;long2");
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
        "subpkstr:string;sub_long:uint32;sub_long2:int32;sub_long3:int64", "sub_pk:primarykey64:subpkstr",
        "subpkstr;sub_long;sub_pack_attr1:sub_long2,sub_long3", "");
    baseSchema->SetSubIndexPartitionSchema(subSchema);

    {
        IndexPartitionSchemaPtr schema(baseSchema->Clone());
        IndexPartitionOptions options;
        options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes = {"pack_attr1", "sub_pack_attr1"};
        ASSERT_THROW(SchemaRewriter::Rewrite(schema, options, GET_PARTITION_DIRECTORY()), IndexCollapsedException);
        const auto& attrSchema = schema->GetAttributeSchema();
        EXPECT_FALSE(attrSchema->GetAttributeConfig("long2")->IsDisabled());
        EXPECT_FALSE(attrSchema->GetAttributeConfig("long3")->IsDisabled());
        EXPECT_FALSE(attrSchema->GetPackAttributeConfig("pack_attr1")->IsDisabled());
    }
    {
        IndexPartitionSchemaPtr schema(baseSchema->Clone());
        IndexPartitionOptions options;
        options.GetOnlineConfig().GetDisableFieldsConfig().attributes = {"long2"};
        ASSERT_THROW(SchemaRewriter::Rewrite(schema, options, GET_PARTITION_DIRECTORY()), IndexCollapsedException);
        const auto& attrSchema = schema->GetAttributeSchema();
        EXPECT_FALSE(attrSchema->GetAttributeConfig("long2")->IsDisabled());
        EXPECT_FALSE(attrSchema->GetAttributeConfig("long3")->IsDisabled());
        EXPECT_FALSE(attrSchema->GetPackAttributeConfig("pack_attr1")->IsDisabled());
    }
}

void SchemaRewriterTest::TestRewriteFieldType()
{
    string field = "nid:uint64;score:float:false:false:fp16";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "nid", "score");
    IndexPartitionOptions options;
    options.SetNeedRewriteFieldType(true);
    ASSERT_THROW(SchemaRewriter::Rewrite(schema, options, GET_PARTITION_DIRECTORY()), IndexCollapsedException);
    auto attrSchema = schema->GetAttributeSchema();
    EXPECT_EQ(FieldType::ft_fp16, attrSchema->GetAttributeConfig("score")->GetFieldType());

    string jsonStr = ToJsonString(schema);
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema());
    FromJsonString(*newSchema, jsonStr);
    attrSchema = newSchema->GetAttributeSchema();
    EXPECT_EQ(FieldType::ft_float, attrSchema->GetAttributeConfig("score")->GetFieldType());
    CompressTypeOption compressType = attrSchema->GetAttributeConfig("score")->GetCompressType();
    ASSERT_TRUE(compressType.HasFp16EncodeCompress());
}

void SchemaRewriterTest::TestRewriteDisableSummary()
{
    IndexPartitionSchemaPtr baseSchema =
        SchemaMaker::MakeSchema("pkstr:string;long1:uint32;long2:int32;long3:int64", "pk:primarykey64:pkstr;",
                                "long1;pack_attr1:long2,long3", "pkstr;long1;long2");
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
        "subpkstr:string;sub_long:uint32;sub_long2:int32;sub_long3:int64", "sub_pk:primarykey64:subpkstr",
        "subpkstr;sub_long;sub_pack_attr1:sub_long2,sub_long3", "");
    baseSchema->SetSubIndexPartitionSchema(subSchema);

    {
        IndexPartitionSchemaPtr schema(baseSchema->Clone());
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        options.GetOnlineConfig().GetDisableFieldsConfig().summarys = DisableFieldsConfig::SDF_FIELD_ALL;
        SchemaRewriter::Rewrite(schema, options, GET_PARTITION_DIRECTORY());
        const auto& summarySchema = schema->GetSummarySchema();
        EXPECT_TRUE(summarySchema->IsAllFieldsDisabled());
        EXPECT_EQ(nullptr, summarySchema->GetSummaryConfig("long1"));
    }
}

void SchemaRewriterTest::TestRewriteDisableSource()
{
    IndexPartitionSchemaPtr baseSchema =
        SchemaMaker::MakeSchema("pkstr:string;long1:uint32;long2:int32;long3:int64", "pk:primarykey64:pkstr;",
                                "long1;pack_attr1:long2,long3", "pkstr;long1;long2", "", "pkstr:long1#long2:long3");
    {
        IndexPartitionSchemaPtr schema(baseSchema->Clone());
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        options.GetOnlineConfig().GetDisableFieldsConfig().sources = DisableFieldsConfig::CDF_FIELD_ALL;
        SchemaRewriter::Rewrite(schema, options, GET_PARTITION_DIRECTORY());
        const auto& sourceSchema = schema->GetSourceSchema();
        EXPECT_TRUE(sourceSchema->IsAllFieldsDisabled());
        EXPECT_TRUE(sourceSchema->GetGroupConfig(0)->IsDisabled());
        EXPECT_TRUE(sourceSchema->GetGroupConfig(1)->IsDisabled());
    }

    {
        IndexPartitionSchemaPtr schema(baseSchema->Clone());
        IndexPartitionOptions options;
        options.SetIsOnline(false);
        options.GetOnlineConfig().GetDisableFieldsConfig().sources = DisableFieldsConfig::CDF_FIELD_GROUP;
        options.GetOnlineConfig().GetDisableFieldsConfig().disabledSourceGroupIds = {0};
        SchemaRewriter::Rewrite(schema, options, GET_PARTITION_DIRECTORY());
        const auto& sourceSchema = schema->GetSourceSchema();
        EXPECT_FALSE(sourceSchema->IsAllFieldsDisabled());
        EXPECT_TRUE(sourceSchema->GetGroupConfig(0)->IsDisabled());
        EXPECT_FALSE(sourceSchema->GetGroupConfig(1)->IsDisabled());
    }
}

void SchemaRewriterTest::TestRewriteBloomFilterParamForPKIndex()
{
    IndexPartitionSchemaPtr baseSchema =
        SchemaMaker::MakeSchema("pkstr:string;long1:uint32;long2:int32;long3:int64", "pk:primarykey64:pkstr;",
                                "long1;pack_attr1:long2,long3", "pkstr;long1;long2", "", "pkstr:long1#long2:long3");

    {
        IndexPartitionSchemaPtr schema(baseSchema->Clone());
        PrimaryKeyIndexConfigPtr pkIndex =
            DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());

        IndexPartitionOptions options;
        options.SetIsOnline(true);
        options.GetBuildConfig().EnableBloomFilterForPkReader(7);
        SchemaRewriter::RewriteForPrimaryKey(schema, options);

        uint32_t multipleNum;
        uint32_t hashFuncNum;
        ASSERT_TRUE(pkIndex->GetBloomFilterParamForPkReader(multipleNum, hashFuncNum));
        ASSERT_EQ(7, multipleNum);
        ASSERT_EQ(5, hashFuncNum);
    }

    {
        IndexPartitionSchemaPtr schema(baseSchema->Clone());
        PrimaryKeyIndexConfigPtr pkIndex =
            DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        pkIndex->SetPrimaryKeyIndexType(pk_hash_table);

        IndexPartitionOptions options;
        options.SetIsOnline(true);
        options.GetBuildConfig().EnableBloomFilterForPkReader(7);
        SchemaRewriter::RewriteForPrimaryKey(schema, options);

        uint32_t multipleNum;
        uint32_t hashFuncNum;
        ASSERT_FALSE(pkIndex->GetBloomFilterParamForPkReader(multipleNum, hashFuncNum));
    }
}

void SchemaRewriterTest::TestEnableBloomFilterForIndexDictionary()
{
    IndexPartitionSchemaPtr baseSchema = SchemaMaker::MakeSchema(
        "pkstr:string;long1:uint32;long2:int32;long3:int64", "pk:primarykey64:pkstr;idx2:number:long1",
        "long1;pack_attr1:long2,long3", "pkstr;long1;long2", "", "pkstr:long1#long2:long3");

    {
        IndexPartitionSchemaPtr schema(baseSchema->Clone());
        IndexPartitionOptions options;
        options.SetIsOnline(true);

        vector<IndexDictionaryBloomFilterParam> params;
        IndexDictionaryBloomFilterParam param;
        param.indexName = "idx2";
        param.multipleNum = 6;
        params.push_back(param);
        options.GetOnlineConfig().SetIndexDictBloomFilterParams(params);

        IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("idx2");
        uint32_t multipleNum = 0;
        uint32_t hashFuncNum = 0;
        ASSERT_FALSE(indexConfig->GetBloomFilterParamForDictionary(multipleNum, hashFuncNum));

        SchemaRewriter::RewriteForEnableBloomFilter(schema, options);
        ASSERT_TRUE(indexConfig->GetBloomFilterParamForDictionary(multipleNum, hashFuncNum));
        ASSERT_EQ(6, multipleNum);
        ASSERT_EQ(4, hashFuncNum);
    }
}

void SchemaRewriterTest::TestRewriteBuildParallelLookupPK()
{
    IndexPartitionSchemaPtr baseSchema =
        SchemaMaker::MakeSchema("pkstr:string;long1:uint32;long2:int32;long3:int64", "pk:primarykey64:pkstr;",
                                "long1;pack_attr1:long2,long3", "pkstr;long1;long2", "", "pkstr:long1#long2:long3");

    {
        IndexPartitionSchemaPtr schema(baseSchema->Clone());
        PrimaryKeyIndexConfigPtr pkIndex =
            DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());

        IndexPartitionOptions options;
        options.SetIsOnline(true);
        options.GetBuildConfig().EnablePkBuildParallelLookup();
        SchemaRewriter::RewriteForPrimaryKey(schema, options);

        ASSERT_TRUE(pkIndex->IsParallelLookupOnBuild());
    }
}

}} // namespace indexlib::index_base
