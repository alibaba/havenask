#include "indexlib/config/test/schema_loader.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SchemaLoader);

SchemaLoader::SchemaLoader() 
{
}

SchemaLoader::~SchemaLoader() 
{
}

IndexPartitionSchemaPtr SchemaLoader::LoadSchema(
        const std::string& root, const std::string& schemaFileName)
{
    string schemaFilePath = FileSystemWrapper::JoinPath(root, schemaFileName);
    string jsonString;
    try
    {
        FileSystemWrapper::AtomicLoad(schemaFilePath, jsonString);
    }
    catch (const misc::NonExistException& e)
    {
        return IndexPartitionSchemaPtr();
    }
    catch (...)
    {
        throw;
    }
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);
    schema->Check();
    return schema;
}

void SchemaLoader::RewriteForRealtimeTimestamp(const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    assert(schema->GetFieldSchema());
    if (schema->GetFieldSchema()->IsFieldNameInSchema(VIRTUAL_TIMESTAMP_FIELD_NAME))
    {
        return;
    }
    schema->AddFieldConfig(VIRTUAL_TIMESTAMP_FIELD_NAME, 
                           VIRTUAL_TIMESTAMP_FIELD_TYPE,
                           false, true);

    IndexConfigPtr indexConfig = IndexConfigCreator::CreateSingleFieldIndexConfig(
            VIRTUAL_TIMESTAMP_INDEX_NAME,
            VIRTUAL_TIMESTAMP_INDEX_TYPE,
            VIRTUAL_TIMESTAMP_FIELD_NAME,
            "", true, schema->GetFieldSchema());
    schema->AddIndexConfig(indexConfig);
    indexConfig->SetOptionFlag(NO_POSITION_LIST);
}


void SchemaLoader::RewriteForRemoveTFBitmapFlag(const IndexPartitionSchemaPtr& schema)
{
    DoRewriteForRemoveTFBitmapFlag(schema);
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
        DoRewriteForRemoveTFBitmapFlag(subSchema);
    }
}

void SchemaLoader::DoRewriteForRemoveTFBitmapFlag(const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    assert(schema->GetIndexSchema());

    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    for (IndexSchema::Iterator iter = indexSchema->Begin();
         iter != indexSchema->End(); ++iter)
    {
        optionflag_t flag = (*iter)->GetOptionFlag();
        if (flag & of_tf_bitmap)
        {
            (*iter)->SetOptionFlag(flag & (~of_tf_bitmap));
        }
    }
}

IE_NAMESPACE_END(config);

