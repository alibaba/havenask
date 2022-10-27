#include "indexlib/index_base/test/schema_rewriter_unittest.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/testlib/schema_maker.h"
#include "indexlib/testlib/modify_schema_maker.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/disable_fields_config.h"

using namespace std;
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SchemaRewriterTest);

SchemaRewriterTest::SchemaRewriterTest()
{
}

SchemaRewriterTest::~SchemaRewriterTest()
{
}

void SchemaRewriterTest::CaseSetUp()
{
}

void SchemaRewriterTest::CaseTearDown()
{
}

void SchemaRewriterTest::TestDisableFields()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "pkstr:string;text1:text;long1:uint32;long2:int32;long3:int64;multi_long:uint32:true;updatable_multi_long:uint32:true:true;field4:location",
        "pk:primarykey64:pkstr;pack1:pack:text1;idx2:number:long1;idx3:text:text1;idx4:spatial:field4",
        "long1;multi_long;updatable_multi_long;pack_attr1:long2,long3",
        "pkstr;");
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
        "substr1:string;subpkstr:string;sub_long:uint32;sub_long2:int32;sub_str2:string;sub_long3:int64;sub_str3:string",
        "subindex1:string:substr1;sub_pk:primarykey64:subpkstr",
        "substr1;subpkstr;sub_long;sub_pack_attr2:sub_long2,sub_str2;sub_pack_attr3:sub_long3,sub_str3",
        "");
    schema->SetSubIndexPartitionSchema(subSchema);
    
    IndexPartitionOptions options;
    options.GetOnlineConfig().GetDisableFieldsConfig().attributes = {
        "long1", "updatable_multi_long"};
    options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes = {
        "pack_attr1", "sub_pack_attr2"};
    options.GetOnlineConfig().GetDisableFieldsConfig().indexes = {
        "idx2", "idx3", "idx4"};
    SchemaRewriter::Rewrite(schema, options, "");
    
    const auto& attrSchema = schema->GetAttributeSchema();
    EXPECT_FALSE(attrSchema->GetAttributeConfig("multi_long")->IsDisable());
    EXPECT_TRUE(attrSchema->GetAttributeConfig("long1")->IsDisable());
    EXPECT_TRUE(attrSchema->GetAttributeConfig("updatable_multi_long")->IsDisable());
    EXPECT_TRUE(attrSchema->GetAttributeConfig("long2")->IsDisable());
    EXPECT_TRUE(attrSchema->GetAttributeConfig("long2")->GetPackAttributeConfig()->IsDisable());
    EXPECT_TRUE(attrSchema->GetAttributeConfig("long3")->IsDisable());
    EXPECT_TRUE(attrSchema->GetAttributeConfig("long3")->GetPackAttributeConfig()->IsDisable());
    EXPECT_TRUE(attrSchema->GetPackAttributeConfig("pack_attr1")->IsDisable());
    for (const auto& config : attrSchema->GetPackAttributeConfig("pack_attr1")->GetAttributeConfigVec())
    {
        EXPECT_TRUE(config->IsDisable());
    }
    
    const auto& subAttrSchema = subSchema->GetAttributeSchema();
    EXPECT_TRUE(subAttrSchema->GetAttributeConfig("sub_long2")->IsDisable());
    EXPECT_TRUE(subAttrSchema->GetAttributeConfig("sub_long2")->GetPackAttributeConfig()->IsDisable());
    EXPECT_TRUE(subAttrSchema->GetAttributeConfig("sub_str2")->IsDisable());
    EXPECT_TRUE(subAttrSchema->GetAttributeConfig("sub_str2")->GetPackAttributeConfig()->IsDisable());
    EXPECT_TRUE(subAttrSchema->GetPackAttributeConfig("sub_pack_attr2")->IsDisable());
    for (const auto& config : subAttrSchema->GetPackAttributeConfig("sub_pack_attr2")->GetAttributeConfigVec())
    {
        EXPECT_TRUE(config->IsDisable());
    }
    
    EXPECT_FALSE(subAttrSchema->GetAttributeConfig("sub_long3")->IsDisable());
    EXPECT_FALSE(subAttrSchema->GetAttributeConfig("sub_long3")->GetPackAttributeConfig()->IsDisable());
    EXPECT_FALSE(subAttrSchema->GetAttributeConfig("sub_str3")->IsDisable());
    EXPECT_FALSE(subAttrSchema->GetAttributeConfig("sub_str3")->GetPackAttributeConfig()->IsDisable());
    EXPECT_FALSE(subAttrSchema->GetPackAttributeConfig("sub_pack_attr3")->IsDisable());
    for (const auto& config : subAttrSchema->GetPackAttributeConfig("sub_pack_attr3")->GetAttributeConfigVec())
    {
        EXPECT_FALSE(config->IsDisable());
    }
    
    const auto& indexSchema = schema->GetIndexSchema();
    EXPECT_TRUE(indexSchema->GetIndexConfig("pk"));
    EXPECT_TRUE(indexSchema->GetIndexConfig("pack1"));
    EXPECT_TRUE(indexSchema->GetIndexConfig("idx2")->IsDisable());
    EXPECT_TRUE(indexSchema->GetIndexConfig("idx3")->IsDisable());
    EXPECT_TRUE(indexSchema->GetIndexConfig("idx4")->IsDisable());
    EXPECT_TRUE(schema->GetSummarySchema());
}

void SchemaRewriterTest::TestRewriteWithModifyOperation()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            "pkstr:string;text1:text;long1:uint32;long2:int32;long3:int64;multi_long:uint32:true;updatable_multi_long:uint32:true:true;",
            "pk:primarykey64:pkstr;pack1:pack:text1;",
            "long1;multi_long;updatable_multi_long;pack_attr1:long2,long3",
            "pkstr;");

    ModifySchemaMaker::AddModifyOperations(schema,
            "", "new_field:uint64;new_field2:uint16", "", "new_field;new_field2");

    IndexPartitionSchemaPtr cloneSchema(schema->Clone());
    IndexPartitionOptions options;
    ASSERT_NO_THROW(SchemaRewriter::Rewrite(cloneSchema, options, ""));
    ASSERT_NO_THROW(SchemaRewriter::RewriteForRealtimeTimestamp(cloneSchema));

    ASSERT_TRUE(cloneSchema->GetIndexSchema()->GetIndexConfig("virtual_timestamp_index"));
}

void SchemaRewriterTest::TestDisableAttributeInSummary()
{
    IndexPartitionSchemaPtr baseSchema = SchemaMaker::MakeSchema(
        "pkstr:string;long1:uint32;long2:int32;long3:int64",
        "pk:primarykey64:pkstr;",
        "long1;pack_attr1:long2,long3",
        "pkstr;long1;long2");
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
        "subpkstr:string;sub_long:uint32;sub_long2:int32;sub_long3:int64",
        "sub_pk:primarykey64:subpkstr",
        "subpkstr;sub_long;sub_pack_attr1:sub_long2,sub_long3",
        "");
    baseSchema->SetSubIndexPartitionSchema(subSchema);

    {
        IndexPartitionSchemaPtr schema(baseSchema->Clone());
        IndexPartitionOptions options;
        options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes = {
            "pack_attr1", "sub_pack_attr1"};
        SchemaRewriter::Rewrite(schema, options, "");
        const auto& attrSchema = schema->GetAttributeSchema();
        EXPECT_FALSE(attrSchema->GetAttributeConfig("long2")->IsDisable());
        EXPECT_FALSE(attrSchema->GetAttributeConfig("long3")->IsDisable());
        EXPECT_FALSE(attrSchema->GetPackAttributeConfig("pack_attr1")->IsDisable());
    }
    {
        IndexPartitionSchemaPtr schema(baseSchema->Clone());
        IndexPartitionOptions options;
        options.GetOnlineConfig().GetDisableFieldsConfig().attributes = {"long2"};
        SchemaRewriter::Rewrite(schema, options, "");
        const auto& attrSchema = schema->GetAttributeSchema();
        EXPECT_FALSE(attrSchema->GetAttributeConfig("long2")->IsDisable());
        EXPECT_FALSE(attrSchema->GetAttributeConfig("long3")->IsDisable());
        EXPECT_FALSE(attrSchema->GetPackAttributeConfig("pack_attr1")->IsDisable());
    }
}

void SchemaRewriterTest::TestRewriteFieldType()
{
    string field = "nid:uint64;score:float:false:false:fp16";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "nid", "score");
    IndexPartitionOptions options;
    options.SetNeedRewriteFieldType(true);
    SchemaRewriter::Rewrite(schema, options, "");
    auto attrSchema = schema->GetAttributeSchema();
    EXPECT_EQ(FieldType::ft_fp16, attrSchema->GetAttributeConfig("score")->GetFieldType());

    string jsonStr = ToJsonString(schema);
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema());
    FromJsonString(*newSchema, jsonStr); 
    attrSchema = newSchema->GetAttributeSchema();
    EXPECT_EQ(FieldType::ft_float, attrSchema->GetAttributeConfig("score")->GetFieldType());
    auto fieldConfig = attrSchema->GetAttributeConfig("score")->GetFieldConfig();
    CompressTypeOption compressType = fieldConfig->GetCompressType();
    ASSERT_TRUE(compressType.HasFp16EncodeCompress());
}

IE_NAMESPACE_END(index_base);

