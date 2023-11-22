#include "indexlib/config/test/schema_loader.h"

#include "indexlib/config/index_config_creator.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
//#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace autil::legacy::json;
using namespace indexlib::file_system;
using namespace indexlib::index;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, SchemaLoader);

SchemaLoader::SchemaLoader() {}

SchemaLoader::~SchemaLoader() {}

IndexPartitionSchemaPtr SchemaLoader::LoadSchema(const std::string& root, const std::string& schemaFileName)
{
    string schemaFilePath = util::PathUtil::JoinPath(root, schemaFileName);
    string jsonString;
    try {
        FslibWrapper::AtomicLoadE(schemaFilePath, jsonString);
    } catch (const util::NonExistException& e) {
        return IndexPartitionSchemaPtr();
    } catch (...) {
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
    if (schema->GetFieldSchema()->IsFieldNameInSchema(VIRTUAL_TIMESTAMP_FIELD_NAME)) {
        return;
    }
    schema->AddFieldConfig(VIRTUAL_TIMESTAMP_FIELD_NAME, VIRTUAL_TIMESTAMP_FIELD_TYPE, false, true);

    IndexConfigPtr indexConfig = IndexConfigCreator::CreateSingleFieldIndexConfig(
        VIRTUAL_TIMESTAMP_INDEX_NAME, VIRTUAL_TIMESTAMP_INDEX_TYPE, VIRTUAL_TIMESTAMP_FIELD_NAME, "", true,
        schema->GetFieldSchema());
    schema->AddIndexConfig(indexConfig);
    indexConfig->SetOptionFlag(NO_POSITION_LIST);
}

void SchemaLoader::RewriteForRemoveTFBitmapFlag(const IndexPartitionSchemaPtr& schema)
{
    DoRewriteForRemoveTFBitmapFlag(schema);
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        DoRewriteForRemoveTFBitmapFlag(subSchema);
    }
}

void SchemaLoader::DoRewriteForRemoveTFBitmapFlag(const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    assert(schema->GetIndexSchema());

    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    for (IndexSchema::Iterator iter = indexSchema->Begin(); iter != indexSchema->End(); ++iter) {
        optionflag_t flag = (*iter)->GetOptionFlag();
        if (flag & of_tf_bitmap) {
            (*iter)->SetOptionFlag(flag & (~of_tf_bitmap));
        }
    }
}
}} // namespace indexlib::config
