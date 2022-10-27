#include "indexlib/partition/test/schema_rewriter_unittest.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/index_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/impl/primary_key_index_config_impl.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/index_base/schema_adapter.h"

using namespace std;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SchemaRewriterTest);

SchemaRewriterTest::SchemaRewriterTest()
{
}

SchemaRewriterTest::~SchemaRewriterTest()
{
}

void SchemaRewriterTest::CaseSetUp()
{
    string field = "pk:string:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint64:true;"
                   "updatable_multi_long:uint64:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "string1;";

    mSchema = SchemaMaker::MakeSchema(field, index,
            attr, summary);
}

void SchemaRewriterTest::CaseTearDown()
{
}

void SchemaRewriterTest::CheckDefragSlicePercent(float defragPercent,
                                                 const IndexPartitionSchemaPtr& schema)
{
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    AttributeSchema::Iterator iter = attrSchema->Begin();
    for (; iter != attrSchema->End(); iter++)
    {
        const AttributeConfigPtr& attrConfig = *iter;
        const FieldConfigPtr& fieldConfig = attrConfig->GetFieldConfig();
        ASSERT_EQ(defragPercent, fieldConfig->GetDefragSlicePercent());
    }
}

void SchemaRewriterTest::TestForRewriteForPrimaryKey()
{
    IndexPartitionOptions options;
    {
        IndexPartitionSchemaPtr schema(mSchema->Clone());
        //test offline speedup pk
        options.SetIsOnline(false);
        options.GetBuildConfig().speedUpPrimaryKeyReader = true;
        SchemaRewriter::RewriteForPrimaryKey(schema, options);
        CheckPrimaryKeySpeedUp(true, schema);
    }

    {
        IndexPartitionSchemaPtr schema(mSchema->Clone());
        //test online not speedup pk
        options.SetIsOnline(true);
        options.GetBuildConfig().speedUpPrimaryKeyReader = false;
        SchemaRewriter::RewriteForPrimaryKey(schema, options);
        CheckPrimaryKeySpeedUp(false, schema);
    }

    {
        IndexPartitionSchemaPtr schema(mSchema->Clone());
        //test online speedup pk
        options.SetIsOnline(true);
        options.GetBuildConfig().speedUpPrimaryKeyReader = true;
        SchemaRewriter::RewriteForPrimaryKey(schema, options);
        CheckPrimaryKeySpeedUp(true, schema);
    }

    {
        //test no pk not core
        string index = "index1:string:string1;pack1:pack:text1;";
        string field = "string1:string;text1:text;";
        IndexPartitionSchemaPtr schema =
            SchemaMaker::MakeSchema(field, index, "", "");
        options.SetIsOnline(true);
        options.GetBuildConfig().speedUpPrimaryKeyReader = true;
        ASSERT_NO_THROW(SchemaRewriter::RewriteForPrimaryKey(schema, options));
    }
}

void SchemaRewriterTest::CheckPrimaryKeySpeedUp(bool speedUp,
        const IndexPartitionSchemaPtr& schema)
{
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    const IndexConfigPtr& indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    const PrimaryKeyIndexConfigPtr& pkIndexConfig = DYNAMIC_POINTER_CAST(
            PrimaryKeyIndexConfig, indexConfig);
    PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode =
        pkIndexConfig->GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode();
    if (speedUp)
    {
        ASSERT_EQ(PrimaryKeyLoadStrategyParam::HASH_TABLE, loadMode);
        return;
    }
    ASSERT_EQ(PrimaryKeyLoadStrategyParam::SORTED_VECTOR, loadMode);
}

IE_NAMESPACE_END(partition);
